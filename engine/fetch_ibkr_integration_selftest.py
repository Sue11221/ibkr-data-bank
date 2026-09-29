"""Port-free A1 integration drives through the real LiveIB choke points."""

from dataclasses import replace
from contextlib import nullcontext
from datetime import date, datetime, time, timedelta
import json
import ast
import inspect
from pathlib import Path
import tempfile
import threading
import textwrap
from types import SimpleNamespace
import unittest
from unittest.mock import patch


def main():
    # Enter through the existing confined owner before production imports.
    # This legacy suite does not mint policy or install a second transport guard.
    from fetch_a2_ibkr_workflows_selftest import Operations
    result = unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite([
        Operations("legacy_integration_contracts_through_confined_transport")]))
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())


import fetch_ibkr_bridge as bridge
import fetch_governors as gov
from fetch_authority import NY, ScheduleAuthority, TOKENS, BAR_SIZES, KINDS, parse_token, CalendarUnsupported
from fetch_run_context import FetchRunContext, RequestRefused, LogicalRequest
from fetch_ledger import LedgerError, inspect_ledger
import stock_ibkr as ib


class InstantPacer:
    """Injected clockless governor; physical attempts still acquire a turn."""
    def __init__(self):
        self.turns = 0
        self.saturations = 0
    def wait_turn(self, cancel=None, metered=True, **kwargs):
        if cancel is not None and cancel.is_set():
            raise ib.Cancelled()
        self.turns += 1
        return 0.000001
    def saturate(self):
        self.saturations += 1


class OfflineLease(nullcontext):
    def release(self):
        pass


def contract(symbol="TEST", conid=123):
    return SimpleNamespace(symbol=symbol, conId=conid, secType="STK",
                           exchange="SMART", currency="USD")


def ib_contract(**kwargs):
    values = vars(contract()).copy()
    values.update(kwargs)
    return SimpleNamespace(**values)


def bar(stamp):
    return SimpleNamespace(date=stamp, open=10, high=11, low=9, close=10, volume=100)


class RawIB:
    def __init__(self, rows=None):
        self.rows = list(rows or [])
        self.calls = []
        self.failures = []
        self.head = datetime(2000, 1, 3, 9, 30, tzinfo=NY)
    def reqHistoricalDataAsync(self, c, **kwargs):
        self.calls.append(("bars", c.conId, kwargs))
        if self.failures:
            raise self.failures.pop(0)
        return self.rows
    def reqHeadTimeStampAsync(self, c, **kwargs):
        self.calls.append(("head", c.conId, kwargs))
        return self.head
    def qualifyContractsAsync(self, c):
        self.calls.append(("qualify", c.symbol))
        if self.failures:
            raise self.failures.pop(0)
        c.conId = 123
        return [c]
    def isConnected(self):
        return True
    def managedAccounts(self):
        return ["TEST-ACCOUNT"]


def assert_empty_pre_month_without_control(case, result, root, events):
    """Shared legacy oracle; the A2 confined owner supplies the actual run."""
    case.assertEqual(result["blocked"], [])
    case.assertEqual(result["unfilled"], ["2026-07-08"])
    case.assertEqual(result["source_absent"], [])
    case.assertEqual(result["written"], 0)
    case.assertFalse((root / "_halted_series.json").exists())
    case.assertEqual({e["payload"]["outcome"] for e in events
                      if e["event"] == "result"}, {"empty"})


class Integration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.authority = ScheduleAuthority.load()

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.pacer = InstantPacer()
        self.virtual_time = 0.0
        def virtual_sleep(seconds, cancel=None):
            if cancel is not None and cancel.is_set():
                raise ib.Cancelled()
            self.virtual_time += seconds
        registry = gov.GovernorRegistry(time_fn=lambda: self.virtual_time, sleep_fn=virtual_sleep)
        self.enterContext(patch.object(gov, "_REGISTRY", registry))
        real_wait, real_saturate = gov.Governor.wait_turn, gov.Governor.saturate
        def counted_wait(governor, *args, **kwargs):
            elapsed = real_wait(governor, *args, **kwargs)
            self.pacer.wait_turn()
            return elapsed
        def counted_saturate(governor):
            self.pacer.saturate()
            return real_saturate(governor)
        self.enterContext(patch.object(gov.Governor, "wait_turn", counted_wait))
        self.enterContext(patch.object(gov.Governor, "saturate", counted_saturate))
        self.enterContext(patch.object(ib, "_default_pacer", return_value=self.pacer))
        self.enterContext(patch.object(ib, "_interruptible_sleep", return_value=None))
        self.adapter = ib.LiveIB()
        self.adapter.ib = RawIB()
        self.adapter._await = lambda response, timeout, what: response
        import operation_gate
        self.enterContext(patch.object(operation_gate, "acquire", side_effect=lambda *a, **k: OfflineLease()))
        self.enterContext(patch.object(ib, "_connection_diag_path", return_value=self.root / "diagnostic.log"))
        self.enterContext(patch.object(ib, "_prevent_sleep", return_value=None))
        self.enterContext(patch.object(ib, "_allow_sleep", return_value=None))
        self.enterContext(patch.object(ib, "_disk_preflight", return_value=True))
        self.enterContext(patch.dict("sys.modules", {"ib_async": SimpleNamespace(
            Stock=lambda symbol, exchange, currency: contract(symbol, 0),
            Contract=ib_contract)}))

    def context(self, now="2026-08-04T17:00:00", *, fault=None, authority=None):
        ctx = FetchRunContext.create(self.root / "ledgers", fault=fault,
            authority=authority or self.authority,
            clock=lambda: datetime.fromisoformat(now).replace(tzinfo=NY))
        self.addCleanup(lambda: None if ctx.ledger._closed else ctx.ledger.close())
        return ctx

    def drive(self, ctx, callback, worker="test-worker"):
        @bridge.worker_scope
        def run(pacer=None, cancel=None):
            return callback()
        return run(pacer=self.pacer, _fetch_worker=ctx.worker(worker))

    def events(self, ctx, seal=True):
        if seal:
            ctx.seal()
        return inspect_ledger(ctx.ledger.path, require_seal=seal)["events"]

    def fetch_session(self, interval="1d", day=date(2026, 7, 8)):
        return list(ib._fetch_sessions([day], interval, self.adapter, contract(),
            self.pacer, None, lambda message: None, lambda: None, "TEST",
            {"requests": 0, "counters": {"invalid": 0, "outside_day": 0, "non_rth": 0}}))

    def test_missing_context_every_live_choke(self):
        attempts = [lambda: self.adapter.fetch(contract(), datetime(2026, 7, 8, 16), "1 D", "1 day"),
                    lambda: self.adapter.head_timestamp(contract()),
                    lambda: self.adapter.qualify("TEST"),
                    lambda: self.adapter.qualify_many(["TEST"]),
                    lambda: self.adapter.company_name("TEST"),
                    lambda: self.adapter.search("TEST")]
        for call in attempts:
            with self.subTest(call=call), self.assertRaises(RequestRefused):
                call()
        self.assertEqual(self.adapter.ib.calls, [])

    def test_worker_scope_alone_is_not_send_permission(self):
        ctx = self.context()
        with self.assertRaises(RequestRefused):
            self.drive(ctx, lambda: self.adapter.qualify("TEST"))
        self.assertEqual(self.adapter.ib.calls, [])

    def test_session_allowed_and_filtered(self):
        ctx = self.context()
        self.adapter.ib.rows = [bar(date(2026, 7, 7)), bar(date(2026, 7, 8)), bar(date(2026, 8, 5))]
        result = self.drive(ctx, self.fetch_session)
        self.assertEqual([b[0].date() for b in result[0][1]], [date(2026, 7, 8)])
        events = self.events(ctx)
        self.assertEqual(events[0]["payload"]["decision"], "allowed")
        self.assertEqual(events[1]["payload"]["observed"]["count"], 3)
        self.assertEqual(events[1]["payload"]["accepted"]["count"], 1)
        self.assertEqual(self.pacer.turns, 1)

    def test_session_clamp_sends_effective_not_requested(self):
        ctx = self.context("2026-07-08T12:00:00")
        self.adapter.ib.rows = [bar(datetime(2026, 7, 8, 11, 58, tzinfo=NY)),
                                bar(datetime(2026, 7, 8, 15, 59, tzinfo=NY))]
        result = self.drive(ctx, lambda: self.fetch_session("1m"))
        events = self.events(ctx)
        decision = events[0]["payload"]
        self.assertEqual(decision["decision"], "clamped")
        sent = self.adapter.ib.calls[0][2]
        self.assertEqual(sent["endDateTime"].isoformat(), decision["effective"]["raw_end"])
        self.assertEqual(sent["durationStr"], decision["effective"]["duration"])
        self.assertEqual(decision["requested"]["intended_start"],
                         decision["effective"]["intended_start"])
        self.assertEqual(len(result[0][1]), 1)

    def test_daily_refused_has_pair_no_send_or_store(self):
        ctx = self.context("2026-07-08T13:20:00")
        sentinel = []
        with self.assertRaises(RequestRefused):
            sentinel.extend(self.drive(ctx, self.fetch_session))
        self.assertEqual(sentinel, [])
        self.assertEqual(self.adapter.ib.calls, [])
        events = self.events(ctx)
        self.assertEqual(events[0]["payload"]["decision"], "refused")
        self.assertEqual(events[1]["payload"]["outcome"], "empty")
        self.assertEqual(self.pacer.turns, 0)

    def test_false_early_daily_producers_never_publish_current_day(self):
        day = date(2026, 7, 1)
        rows = dict(self.authority.rows)
        rows[day] = replace(rows[day], status="open_early_close", close=time(13))
        authority = replace(self.authority, rows=rows)
        for producer in ("session", "month", "probe"):
            with self.subTest(producer=producer):
                ctx = self.context("2026-07-01T13:20:00", authority=authority)
                self.adapter.ib.rows = [bar(day)]
                self.adapter.ib.calls.clear()
                if producer == "session":
                    call = lambda: self.fetch_session(day=day)
                elif producer == "month":
                    call = lambda: ib._fetch_month_bars(self.adapter, contract(), 2026, 7, "1d")
                else:
                    call = lambda: ib._probe_earliest_daily(self.adapter, contract(), day)
                if producer == "probe":
                    self.assertIsNone(self.drive(ctx, call))  # Earlier settled history may be probed.
                else:
                    with self.assertRaises(RequestRefused):
                        self.drive(ctx, call)
                    self.assertEqual(self.adapter.ib.calls, [])
                events = self.events(ctx)
                for event in events:
                    if event["event"] == "result" and "accepted" in event["payload"]:
                        self.assertEqual(event["payload"]["accepted"]["count"], 0)

    def test_month_daily_allowed_clamped_refused(self):
        for now, state in (("2026-08-04T17:00:00", "allowed"),
                           ("2026-07-31T17:00:00", "clamped"),
                           ("2026-06-30T17:00:00", "refused")):
            with self.subTest(state=state):
                ctx = self.context(now)
                self.adapter.ib.calls.clear()
                call = lambda: ib._fetch_month_bars(self.adapter, contract(), 2026, 7, "1d")
                if state == "refused":
                    with self.assertRaises(RequestRefused):
                        self.drive(ctx, call)
                    self.assertEqual(self.adapter.ib.calls, [])
                else:
                    self.drive(ctx, call)
                events = self.events(ctx)
                self.assertEqual(events[0]["payload"]["decision"], state)
                self.assertEqual(events[0]["producer_id"], "ibkr.gap_fill.month_daily")

    def test_month_intraday_allowed_clamped_refused(self):
        for now, state in (("2026-08-04T17:00:00", "allowed"),
                           ("2026-07-31T17:00:00", "clamped"),
                           ("2026-06-30T17:00:00", "refused")):
            with self.subTest(state=state):
                ctx = self.context(now)
                self.adapter.ib.calls.clear()
                before = self.pacer.turns
                call = lambda: ib._fetch_month_bars(self.adapter, contract(), 2026, 7, "1m")
                if state == "refused":
                    with self.assertRaises(RequestRefused):
                        self.drive(ctx, call)
                    self.assertEqual(self.adapter.ib.calls, [])
                else:
                    self.drive(ctx, call)
                events = self.events(ctx)
                self.assertEqual(events[0]["payload"]["decision"], state)
                self.assertEqual(events[0]["producer_id"], "ibkr.gap_fill.month_intraday")
                self.assertEqual(self.pacer.turns - before, len(self.adapter.ib.calls))

    def test_extended_half_day_month_preserves_unsupported_debt(self):
        ctx = self.context()
        bars, skipped = self.drive(ctx, lambda:
            ib._fetch_month_bars(self.adapter, contract(), 2025, 11, "1m-pre"))
        self.assertEqual(bars, [])
        self.assertEqual(skipped, {date(2025, 11, 28)})
        events = self.events(ctx)
        decisions = [event["payload"] for event in events if event["event"] == "decision"]
        self.assertEqual(len(decisions), len(self.adapter.ib.calls))
        self.assertGreater(len(decisions), 0)
        self.assertTrue(all(value["decision"] == "allowed" for value in decisions))
        for value in decisions:
            requested = value["requested"]
            first = datetime.fromisoformat(requested["intended_start"]).date()
            last = datetime.fromisoformat(requested["intended_end"]).date()
            windows = [ctx.authority.window("1m-pre", first + timedelta(days=offset))
                       for offset in range((last - first).days + 1)]
            self.assertTrue(any(window is not None for window in windows))
        with self.assertRaises(CalendarUnsupported):
            ctx.authority.window("1m-pre", date(2025, 11, 28))

    def test_retry_same_logical_fresh_attempt_pairs(self):
        ctx = self.context()
        self.adapter.ib.failures = [ib.RequestTimeout("injected timeout")]
        self.drive(ctx, self.fetch_session)
        events = self.events(ctx)[:-1]
        self.assertEqual(len(events), 4)
        self.assertEqual(events[0]["logical_id"], events[2]["logical_id"])
        self.assertNotEqual(events[0]["attempt_id"], events[2]["attempt_id"])
        self.assertEqual(events[1]["payload"]["outcome"], "timeout")
        self.assertEqual(self.pacer.turns, 2)

    def test_decision_and_result_failures_prevent_publication(self):
        for kind in ("decision", "result"):
            for stage in ("append", "flush", "fsync"):
                with self.subTest(kind=kind, stage=stage):
                    def fault(event, phase):
                        if event == kind and phase == stage:
                            raise OSError("injected journal failure")
                    ctx = self.context(fault=fault)
                    self.adapter.ib.rows = [bar(date(2026, 7, 8))]
                    self.adapter.ib.calls.clear()
                    cache = []
                    with self.assertRaises(LedgerError):
                        cache.extend(self.drive(ctx, self.fetch_session))
                    self.assertEqual(cache, [])
                    self.assertEqual(len(self.adapter.ib.calls), int(kind == "result"))
                    self.assertFalse(ctx.ledger.verified)

    def test_head_paths_have_ledger_only_metadata(self):
        for producer in ("start", "earliest"):
            ctx = self.context()
            if producer == "start":
                self.drive(ctx, lambda: ib._head_start_evidence(self.adapter, contract(), None,
                                                              date(2026, 7, 8)))
            else:
                self.drive(ctx, lambda: ib.earliest_available(self.adapter, "TEST", date(2026, 7, 8)))
            events = self.events(ctx)
            heads = [e for e in events if e["event"] == "decision"
                     and e["payload"]["requested"]["variant"] == "ibkr-head"]
            self.assertEqual(len(heads), 1)
            self.assertIsNone(heads[0]["payload"]["horizon"])
            self.assertNotIn("raw_end", heads[0]["payload"]["requested"])

    def test_qualification_expansion_and_fallback_pairs(self):
        ctx = self.context()
        self.adapter.ib.failures = [ValueError("retry one contract")]
        def call():
            with bridge.qualification_scope("qualify_many", ["ONE", "TWO"], bridge.acquire_turn):
                return self.adapter.qualify_many(["ONE", "TWO"])
        self.assertEqual(self.drive(ctx, call), {"ONE": 123, "TWO": 123})
        events = self.events(ctx)[:-1]
        self.assertEqual(len(events), 6)
        self.assertEqual(events[0]["logical_id"], events[2]["logical_id"])
        self.assertNotEqual(events[2]["logical_id"], events[4]["logical_id"])
        self.assertEqual(self.pacer.turns, 3)

    def test_bar_normalizers_reject_ambiguous_timestamp(self):
        ctx = self.context()
        self.adapter.ib.rows = [bar(datetime(2026, 7, 8, 9, 30))]
        with self.assertRaises(ValueError):
            self.drive(ctx, lambda: self.fetch_session("1m"))
        self.assertEqual(self.events(ctx)[1]["payload"]["outcome"], "error")

    def test_scope_consumed_before_observer_can_reenter(self):
        ctx = self.context()
        reentry = []
        def acquire(cancel=None, metered=True, **kwargs):
            if reentry:  # Bound the inverse: no recursive observer loop.
                return 0.1
            reentry.append(True)
            with self.assertRaises(RequestRefused):
                self.adapter.fetch(contract(), datetime(2026, 7, 8, 16), "1 D", "1 day")
            return 0.1
        self.pacer.wait_turn = acquire
        self.drive(ctx, lambda: ib._fetch_request(self.adapter, contract(),
            datetime(2026, 7, 8, 16), "1 D", "1 day", self.pacer, None,
            lambda _: None, lambda: None, "TEST", {"requests": 0}, "day", interval="1d"))
        self.assertEqual(reentry, [True])
        self.assertEqual(len(self.adapter.ib.calls), 1)

    def test_http_seams_refuse_even_injected_transport(self):
        import stock_validate
        import external_sweep
        import split_provider
        import sp500
        calls = []
        opener = lambda *args, **kwargs: calls.append(True)
        attempts = [lambda: stock_validate.fetch_daily_reference("A1-REFUSAL", _opener=opener),
                    lambda: external_sweep.StockAnalysisProvider(opener=opener)._read("test"),
                    lambda: split_provider._default_fetcher("test"),
                    lambda: sp500._http("test", 1, opener)]
        for call in attempts:
            with self.assertRaises(RequestRefused):
                call()
        self.assertEqual(calls, [])

    def operation_fixture(self, *, recovering=False, fault=None):
        created, workers, adapters = [], [], {}
        failed = set()
        real_create = FetchRunContext.create
        def create(directory):
            ctx = real_create(directory, authority=self.authority,
                clock=lambda: datetime(2026, 7, 8 + len(created), 17, tzinfo=NY), fault=fault)
            created.append(ctx)
            return ctx
        def make(host, ports):
            port = ports[0]
            def connect():
                if recovering and port == 1001 and port not in failed:
                    failed.add(port)
                    raise ConnectionError("offline injected first-pass failure")
                if port not in adapters:
                    adapter = ib.LiveIB()
                    adapter.port = port
                    adapter.ib = RawIB([bar(date(2026, 7, 8))])
                    adapter._await = lambda response, timeout, what: response
                    adapters[port] = ib.ReusableAdapter(lambda: adapter)
                return adapters[port]
            return connect
        def fill(root, adapter, pacer, ticker, interval, *args, **kwargs):
            worker = bridge.current_worker()
            workers.append((worker.context, worker.worker_id))
            res = {"ticker": ticker, "interval": interval, "requests": 0,
                   "counters": {"invalid": 0, "outside_day": 0, "non_rth": 0}, "notes": []}
            output = list(ib._fetch_sessions([date(2026, 7, 8)], interval,
                adapter, contract(ticker), pacer, None, lambda _: None, lambda: None,
                ticker, res))
            res["bars_fetched"] = sum(len(bars) for day, bars in output)
            return res
        return created, workers, adapters, create, make, fill

    def test_actual_parallel_workers_share_context_and_join_before_seal(self):
        created, workers, adapters, create, make, fill = self.operation_fixture()
        with patch.object(FetchRunContext, "create", side_effect=create), patch.object(ib, "_fill_series", side_effect=fill):
            report = ib.nightly_gap_fill_parallel(self.root, [("ONE", "1d"), ("TWO", "1d")],
                [1001, 1002], adapter_factory=make)
        self.assertEqual(len(created), 1)
        self.assertEqual(len(workers), 2)
        self.assertTrue(all(ctx is created[0] for ctx, _ in workers))
        self.assertTrue(report["fetch_ledger"]["verified"])
        parsed = inspect_ledger(created[0].ledger.path)
        decisions = [e for e in parsed["events"] if e["event"] == "decision"]
        self.assertGreaterEqual(len(decisions), 4)  # Each symbol qualifies and fetches.
        self.assertEqual({e["operation_id"] for e in decisions}, {created[0].operation_id})
        self.assertEqual(len({e["payload"]["captured_now"] for e in decisions}), 1)
        self.assertFalse(any(t.name.startswith("ibkr-fetch-") and t.is_alive() for t in threading.enumerate()))

    def test_actual_resilient_recovery_retains_context_unique_worker_ids(self):
        created, workers, adapters, create, make, fill = self.operation_fixture(recovering=True)
        with patch.object(FetchRunContext, "create", side_effect=create), patch.object(ib, "_fill_series", side_effect=fill):
            report = ib.nightly_gap_fill_parallel_resilient(self.root,
                [("ONE", "1d"), ("TWO", "1d")], [1001, 1002],
                adapter_factory=make, restart_port=lambda port: True,
                port_up=lambda port: True, max_recover_rounds=1)
        self.assertEqual(report.get("recovery_rounds"), 1)
        self.assertEqual(len(created), 1)
        self.assertGreaterEqual(len(workers), 2)
        self.assertTrue(all(ctx is created[0] for ctx, _ in workers))
        self.assertEqual(len({worker for _, worker in workers}), len(workers))
        self.assertTrue(report["fetch_ledger"]["verified"])

    def test_reusable_adapter_gets_fresh_context_each_operation(self):
        created, workers, adapters, create, make, fill = self.operation_fixture()
        with patch.object(FetchRunContext, "create", side_effect=create), patch.object(ib, "_fill_series", side_effect=fill):
            reports = [ib.nightly_gap_fill_parallel(self.root, [("ONE", "1d")], [1001],
                adapter_factory=make) for _ in range(2)]
        self.assertEqual(len(created), 2)
        self.assertIsNot(workers[0][0], workers[1][0])
        self.assertNotEqual(created[0].horizons["1d"], created[1].horizons["1d"])
        self.assertEqual(len(adapters), 1)
        self.assertTrue(all(report["fetch_ledger"]["verified"] for report in reports))
        live = adapters[1001]._a
        self.assertIsInstance(live, ib.LiveIB)
        self.assertFalse(any(isinstance(value, FetchRunContext) for value in vars(live).values()))

    def test_seal_failure_visible_in_return_and_saved_report(self):
        def fault(event, phase):
            if event == "seal" and phase == "fsync":
                raise OSError("injected seal failure")
        created, workers, adapters, create, make, fill = self.operation_fixture(fault=fault)
        with patch.object(FetchRunContext, "create", side_effect=create), patch.object(ib, "_fill_series", side_effect=fill):
            report = ib.nightly_gap_fill_parallel(self.root, [("ONE", "1d")], [1001], adapter_factory=make)
        self.assertEqual(report["fetch_ledger"]["state"], "UNVERIFIED")
        saved = json.loads(Path(report["report_path"]).read_text(encoding="utf-8"))
        self.assertEqual(saved["fetch_ledger"], report["fetch_ledger"])
        with self.assertRaises(LedgerError):
            inspect_ledger(created[0].ledger.path)

    def test_clamped_future_daily_suffix_is_not_empty_session_evidence(self):
        ctx = self.context("2026-07-08T13:20:00")
        self.adapter.ib.rows = [bar(date(2026, 7, 7)), bar(date(2026, 7, 8))]
        res = {"requests": 0, "counters": {"invalid": 0, "outside_day": 0, "non_rth": 0}}
        published = []
        def call():
            for item in ib._fetch_sessions([date(2026, 7, 7), date(2026, 7, 8)], "1d",
                    self.adapter, contract(), self.pacer, None, lambda _: None,
                    lambda: None, "TEST", res):
                published.append(item)
        with self.assertRaisesRegex(RequestRefused, "not source-absent"):
            self.drive(ctx, call)
        self.assertEqual([day for day, bars in published], [date(2026, 7, 7)])
        self.assertEqual(res["horizon_deferred_days"], ["2026-07-08"])

    def test_month_future_suffix_is_unknown_not_source_absent(self):
        ctx = self.context("2026-07-08T13:20:00")
        self.adapter.ib.rows = [bar(date(2026, 7, 7)), bar(date(2026, 7, 8))]
        bars, skipped = self.drive(ctx, lambda: ib._fetch_month_bars(self.adapter, contract(), 2026, 7, "1d"))
        self.assertEqual([b[0].date() for b in bars], [date(2026, 7, 7)])
        self.assertIn(date(2026, 7, 8), skipped)
        self.assertNotIn(date(2026, 7, 7), skipped)

    def test_empty_qualification_is_ledgered_not_retry_or_success(self):
        ctx = self.context()
        with patch.object(self.adapter.ib, "qualifyContractsAsync", return_value=[None]) as raw:
            def call():
                with bridge.qualification_scope("qualify_many", ["NONE"], bridge.acquire_turn):
                    return self.adapter.qualify_many(["NONE"])
            self.assertEqual(self.drive(ctx, call), {"NONE": None})
        self.assertEqual(raw.call_count, 1)
        self.assertEqual(self.events(ctx)[1]["payload"]["outcome"], "empty")

    def test_unavailable_head_falls_back_to_ledgered_daily_probe(self):
        ctx = self.context()
        self.adapter.ib.head = None
        self.adapter.ib.rows = [bar(date(2026, 7, 8))]
        result = self.drive(ctx, lambda: ib._head_start_evidence(self.adapter, contract(), None,
                                                               date(2026, 7, 8)))
        self.assertEqual(result[2], date(2026, 7, 8))
        events = self.events(ctx)
        self.assertEqual(events[1]["payload"]["error_type"], "SeriesHalt")
        self.assertEqual(events[2]["producer_id"], "ibkr.gap_fill.head_daily_probe")

    def test_qualification_publication_uses_durable_snapshot(self):
        held = []
        def raw(c):
            c.conId = 123
            held.append(c)
            return [c]
        def fault(event, stage):
            if event == "result" and stage == "fsync":
                held[0].conId = 999
        ctx = self.context(fault=fault)
        def call():
            with bridge.qualification_scope("qualify", ["TEST"], bridge.acquire_turn):
                return self.adapter.qualify("TEST")
        with patch.object(self.adapter.ib, "qualifyContractsAsync", side_effect=raw):
            conid, published = self.drive(ctx, call)
        self.assertEqual((conid, published.conId), (123, 123))
        self.assertIsNot(published, held[0])

    def test_wait_callback_cannot_retarget_contract(self):
        ctx = self.context()
        original = contract()
        def acquire(cancel=None, metered=True, **kwargs):
            original.conId = 999
            return 0.1
        self.pacer.wait_turn = acquire
        self.drive(ctx, lambda: ib._fetch_request(self.adapter, original,
            datetime(2026, 7, 8, 16), "1 D", "1 day", self.pacer, None,
            lambda _: None, lambda: None, "TEST", {"requests": 0}, "day", interval="1d"))
        self.assertEqual(self.adapter.ib.calls[0][1], 123)

    def test_external_callback_cannot_borrow_worker_for_shared_probe(self):
        ctx = self.context()
        def callback():
            ib._probe_earliest_daily(self.adapter, contract(), date(2026, 7, 8))
        with self.assertRaises(RequestRefused):
            self.drive(ctx, bridge.without_authority(callback))
        self.assertEqual(self.adapter.ib.calls, [])

    def test_cancelled_qualification_has_no_hidden_retry(self):
        ctx = self.context()
        def cancelled(*args, **kwargs):
            raise ib.Cancelled()
        self.pacer.wait_turn = cancelled
        def call():
            with bridge.qualification_scope("qualify_many", ["ONE", "TWO"], bridge.acquire_turn):
                return self.adapter.qualify_many(["ONE", "TWO"])
        with self.assertRaises(ib.Cancelled):
            self.drive(ctx, call)
        self.assertEqual(self.adapter.ib.calls, [])

    def test_month_pacing_violation_saturates_then_new_attempt(self):
        ctx = self.context()
        self.adapter.ib.failures = [ib.PacingViolation("injected saturation")]
        self.drive(ctx, lambda: ib._fetch_month_bars(self.adapter, contract(), 2026, 7, "1m"))
        events = self.events(ctx)
        self.assertEqual(self.pacer.saturations, 1)
        self.assertEqual(events[0]["logical_id"], events[2]["logical_id"])
        self.assertNotEqual(events[0]["attempt_id"], events[2]["attempt_id"])
        self.assertEqual(self.pacer.turns, len(self.adapter.ib.calls))

    def test_session_violation_saturates_canonical_account_not_legacy_pacer(self):
        ctx = self.context()
        self.adapter.ib.failures = [ib.PacingViolation("injected session saturation")]
        self.drive(ctx, self.fetch_session)
        governor = ctx.governors.ibkr(["TEST-ACCOUNT"])
        self.assertTrue(governor._pacer._forced)
        self.assertEqual(len(self.adapter.ib.calls), 2)
        events = self.events(ctx)
        self.assertEqual(events[0]["logical_id"], events[2]["logical_id"])
        self.assertNotEqual(events[0]["attempt_id"], events[2]["attempt_id"])
        self.assertGreater(events[2]["payload"]["pacer_wait_seconds"], .15)

    def test_session_saturation_misrouting_inverse(self):
        mutant = self._mutant(ib, "_fetch_request", "fib.pacer().saturate()", "pacer.saturate()")
        with patch.object(ib, "_fetch_request", mutant), self.assertRaises(AssertionError):
            self.test_session_violation_saturates_canonical_account_not_legacy_pacer()

    def test_probe_allowed_clamped_and_out_of_coverage_refusal(self):
        for now, day, state in (("2026-08-04T17:00:00", date(2026, 7, 8), "allowed"),
                                ("2026-07-08T13:20:00", date(2026, 7, 8), "clamped"),
                                ("2026-08-04T17:00:00", date(2090, 7, 8), "calendar_unsupported")):
            ctx = self.context(now)
            self.adapter.ib.calls.clear()
            call = lambda: ib._probe_earliest_daily(self.adapter, contract(), day)
            if state == "calendar_unsupported":
                with self.assertRaises(RequestRefused):
                    self.drive(ctx, call)
                self.assertEqual(self.adapter.ib.calls, [])
            else:
                self.drive(ctx, call)
            self.assertEqual(self.events(ctx)[0]["payload"]["decision"], state)

    def test_inverse_filter_bypass_returns_unsettled_daily_bar(self):
        ctx = self.context("2026-07-08T13:20:00")
        self.adapter.ib.rows = [bar(date(2026, 7, 8))]
        import fetch_run_context
        with patch.object(fetch_run_context, "filter_rows", side_effect=lambda rows, *a: rows):
            output = self.drive(ctx, lambda: ib._probe_earliest_daily(self.adapter, contract(), date(2026, 7, 8)))
        self.assertEqual(output, date(2026, 7, 8))  # The stock oracle expects None: RED.

    def test_inverse_choke_guard_bypass_reaches_raw_send(self):
        class Bypass:
            envelope = SimpleNamespace(token="1d")
            def execute(inner, transport, **kwargs):
                from fetch_envelopes import parse_envelope
                effective = parse_envelope({"variant": "ibkr-bars", "symbol": "TEST", "con_id": 123,
                    "token": "1d", "what_to_show": "TRADES", "use_rth": True, "bar_size": "1 day",
                    "raw_end": datetime(2026, 7, 8, 16, tzinfo=NY), "duration": "1 D",
                    "intended_start": datetime(2026, 7, 8, tzinfo=NY),
                    "intended_end": datetime(2026, 7, 8, 16, tzinfo=NY)})
                transport(effective)
                return []
        with patch.object(bridge, "take", return_value=bridge.Attempt(Bypass(), lambda: 0)), \
                patch.object(bridge, "bind_request", side_effect=lambda adapter, request: request):
            self.adapter.fetch(contract(), datetime(2026, 7, 8, 16), "1 D", "1 day")
        self.assertEqual(len(self.adapter.ib.calls), 1)  # Stock missing-context oracle expects zero.

    def test_inverse_month_governor_bypass_is_detected(self):
        ctx = self.context()
        # Producer-only bypass is now redundant: the live choke reserves anyway.
        with patch.object(bridge, "acquire_turn", return_value=0):
            self.drive(ctx, lambda: ib._fetch_month_bars(self.adapter, contract(), 2026, 7, "1m"))
        self.assertGreater(len(self.adapter.ib.calls), 0)
        self.assertEqual(self.pacer.turns, len(self.adapter.ib.calls))
        self.adapter.ib.calls.clear()
        self.pacer.turns = 0
        def bypass(bound, transport, *, acquire_turn, normalizer=None):
            return bound.request.execute(transport, acquire_turn=acquire_turn, normalizer=normalizer)
        with patch.object(bridge, "acquire_turn", return_value=0), \
                patch.object(bridge.BoundRequest, "execute", bypass):
            self.drive(ctx, lambda: ib._fetch_month_bars(self.adapter, contract(), 2026, 7, "1m"))
        self.assertGreater(len(self.adapter.ib.calls), 0)
        self.assertNotEqual(self.pacer.turns, len(self.adapter.ib.calls))

    def test_inverse_context_recomputation_breaks_real_recovery_identity(self):
        # Inverse of explicit parent forwarding: a nested fresh nightly root
        # replaces the shared helper. This changes no runtime mint registry.
        original = ib.gap_fill_parallel
        def mutated(*args, **kwargs):
            kwargs.pop("_fetch_parent", None)
            with patch.object(ib, "gap_fill_parallel", original):
                return ib.nightly_gap_fill_parallel(*args, **kwargs)
        created, workers, adapters, create, make, fill = self.operation_fixture(recovering=True)
        with patch.object(ib, "gap_fill_parallel", mutated), patch.object(FetchRunContext, "create", side_effect=create), patch.object(ib, "_fill_series", side_effect=fill):
            ib.nightly_gap_fill_parallel_resilient(self.root, [("ONE", "1d"), ("TWO", "1d")],
                [1001, 1002], adapter_factory=make, restart_port=lambda p: True,
                port_up=lambda p: True, max_recover_rounds=1)
        self.assertGreater(len(created), 1)
        self.assertGreater(len({id(ctx) for ctx, _ in workers}), 1)

    def seed_daily(self, interval="1d", day=date(2026, 7, 7)):
        ss = ib.ss
        path = ss.month_file_path(self.root, "TEST", day.year, day.month, interval)
        hour, minute = ((0, 0) if interval == "1d" else
                        (16, 0) if interval.endswith("-post") else
                        (4, 0) if interval.endswith("-pre") else (9, 30))
        stamp = datetime.combine(day, time(hour, minute))
        stats = ss.write_month_file(path, [(stamp, 10, 11, 9, 10, 100)])
        manifest = ss.new_manifest("TEST", "TEST")
        manifest["conid"] = 123
        ss.manifest_months(manifest, interval)[ss.month_key(day.year, day.month)] = dict(
            stats, status="present", source="fixture")
        ss.save_manifest(self.root / "TEST", manifest)
        return path

    def test_actual_store_unchanged_after_result_failure(self):
        path = self.seed_daily()
        before = path.read_bytes()
        contexts = []
        def fault(event, phase):
            pending = contexts[0].ledger._pending.values()
            if (event == "result" and phase == "fsync"
                    and any(ids[2] == "ibkr.gap_fill.session_fetch" for ids in pending)):
                raise OSError("injected bar result fsync failure")
        ctx = self.context("2026-07-08T17:00:00", fault=fault)
        contexts.append(ctx)
        self.adapter.ib.rows = [bar(date(2026, 7, 7)), bar(date(2026, 7, 8))]
        with patch.object(FetchRunContext, "create", return_value=ctx), patch.object(ib, "_commit_month", wraps=ib._commit_month) as commit:
            report = ib.nightly_gap_fill_parallel(self.root, [("TEST", "1d")], [1001],
                adapter_factory=lambda *a: lambda: self.adapter)
        self.assertEqual(commit.call_count, 0)
        self.assertEqual(path.read_bytes(), before)
        self.assertFalse(report["fetch_ledger"]["verified"])
        self.assertIn("fsync", report["series"][0]["halt"])

    def test_actual_daily_store_under_false_early_table_excludes_current_day(self):
        path = self.seed_daily()
        day = date(2026, 7, 8)
        rows = dict(self.authority.rows)
        rows[day] = replace(rows[day], status="open_early_close", close=time(13))
        ctx = self.context("2026-07-08T13:20:00", authority=replace(self.authority, rows=rows))
        self.adapter.ib.rows = [bar(date(2026, 7, 7)), bar(day)]
        with patch.object(FetchRunContext, "create", return_value=ctx):
            report = ib.nightly_gap_fill_parallel(self.root, [("TEST", "1d")], [1001],
                adapter_factory=lambda *a: lambda: self.adapter, pipeline=True)
        stored, _ = ib.ss.read_month_file(path)
        self.assertEqual({b[0].date() for b in stored}, {date(2026, 7, 7)})
        self.assertIsNone(report["series"][0]["halt"])
        self.assertEqual(report["series"][0]["horizon_deferred_days"], [day.isoformat()])
        self.assertTrue(report["fetch_ledger"]["verified"])

    def test_cancel_after_durable_decision_has_no_successful_seal(self):
        created, workers, adapters, create, make, fill = self.operation_fixture()
        def interrupted(*args, **kwargs):
            self.adapter.ib.reqHistoricalDataAsync = lambda *a, **k: (_ for _ in ()).throw(KeyboardInterrupt())
            return fill(args[0], self.adapter, *args[2:], **kwargs)
        with patch.object(FetchRunContext, "create", side_effect=create), patch.object(ib, "_fill_series", side_effect=interrupted):
            with self.assertRaises(KeyboardInterrupt):
                ib.nightly_gap_fill_parallel(self.root, [("ONE", "1d")], [1001], adapter_factory=make)
        self.assertEqual(len(created), 1)
        parsed = inspect_ledger(created[0].ledger.path, require_seal=False)
        self.assertEqual(len(parsed["outcome_unknown"]), 1)
        self.assertFalse(parsed["verified"])
        report_files = list((self.root / "_ingest_reports").rglob("*.operation.json"))
        self.assertTrue(any(json.loads(path.read_text(encoding="utf-8")).get("state")
                            == "UNVERIFIED" for path in report_files))

    def test_all_132_token_mappings_at_actual_bar_choke(self):
        ctx = self.context()
        accepted = refused = 0
        for token in TOKENS:
            with self.subTest(token=token):
                base, kind, session = parse_token(token)
                end = datetime(2026, 7, 8, 16 if session == "rth" else 20 if session == "post" else 9, 30 if session == "pre" else 0)
                start = datetime(2026, 7, 8)
                self.adapter.ib.calls.clear()
                call = lambda: ib._fetch_request(self.adapter, contract(), end, "1 D", BAR_SIZES[base][0],
                    self.pacer, None, lambda _: None, lambda: None, "TEST", {"requests": 0}, "fixture",
                    metered=base.endswith("s"), use_rth=session == "rth", what_to_show=KINDS[kind],
                    interval=token, intended_start=start)
                if base == "1d" and session != "rth":
                    with self.assertRaises(RequestRefused):
                        self.drive(ctx, call)
                    self.assertEqual(self.adapter.ib.calls, [])
                    refused += 1
                else:
                    self.drive(ctx, call)
                    sent = self.adapter.ib.calls[0][2]
                    self.assertEqual((sent["barSizeSetting"], sent["whatToShow"], sent["useRTH"]),
                                     (BAR_SIZES[base][0], KINDS[kind], session == "rth"))
                    accepted += 1
        self.assertEqual((accepted, refused), (124, 8))
        self.assertEqual(len(self.events(ctx)), 265)

    def test_clock_once_and_no_midrun_settlement_advance(self):
        readings = [datetime(2026, 7, 8, 16, 19, 59, tzinfo=NY)]
        calls = []
        def clock():
            calls.append(True)
            return readings[0]
        ctx = FetchRunContext.create(self.root / "clock-ledgers", authority=self.authority, clock=clock)
        self.addCleanup(ctx.ledger.close)
        before = dict(ctx.horizons)
        def head(c, **kwargs):
            readings[0] = datetime(2026, 7, 8, 16, 20, 1, tzinfo=NY)
            return datetime(2000, 1, 3, tzinfo=NY)
        with patch.object(self.adapter.ib, "reqHeadTimeStampAsync", side_effect=head), patch.object(ScheduleAuthority, "horizon", side_effect=AssertionError("worker recomputed horizon")):
            self.drive(ctx, lambda: ib._head_start_evidence(self.adapter, contract(), None, date(2026, 7, 8)))
            with self.assertRaises(RequestRefused):
                self.drive(ctx, self.fetch_session)
        self.assertEqual(calls, [True])
        self.assertEqual(dict(ctx.horizons), before)
        self.assertEqual(ctx.horizons["1d"].date(), date(2026, 7, 7))

    def test_post_pipeline_cannot_borrow_a1_worker(self):
        observations = []
        raw = self.adapter.ib
        class Post:
            pending = True
            def record_series(self, rows):
                pass
            def claim(self):
                if self.pending:
                    self.pending = False
                    return ("probe", "TEST")
                return None
            def has_pending(self):
                return self.pending
            def execute(inner, task, adapter, pacer):
                observations.append(bridge.current_worker())
                with self.assertRaises(RequestRefused):
                    ib._fetch_month_bars(adapter, contract(), 2026, 7, "1d")
        ib.nightly_gap_fill(self.root, [], adapter_factory=lambda: self.adapter, pacer=self.pacer,
                    post_pipeline=Post())
        self.assertEqual(observations, [None])
        self.assertEqual(raw.calls, [])

    def test_qualification_retry_resets_mutated_lookup_contract(self):
        ctx = self.context()
        inputs = []
        def raw(c):
            inputs.append(c.conId)
            if len(inputs) == 1:
                c.conId = 999
                raise ValueError("provider mutated contract then failed")
            c.conId = 123
            return [c]
        def call():
            with bridge.qualification_scope("qualify_many", ["TEST"], bridge.acquire_turn):
                return self.adapter.qualify_many(["TEST"])
        with patch.object(self.adapter.ib, "qualifyContractsAsync", side_effect=raw):
            self.assertEqual(self.drive(ctx, call), {"TEST": 123})
        self.assertEqual(inputs, [0, 0])

    def test_retry_reapplies_extended_session_after_reconnect(self):
        ctx = self.context()
        self.adapter.ib.failures = [ConnectionError("injected link loss")]
        with patch.object(self.adapter, "reconnect", side_effect=lambda: setattr(self.adapter, "use_rth", True)):
            self.drive(ctx, lambda: self.fetch_session("1m-post"))
        self.assertEqual([call[2]["useRTH"] for call in self.adapter.ib.calls], [False, False])

    def _mutant(self, module, name, needle, replacement):
        source = textwrap.dedent(inspect.getsource(getattr(module, name)))
        self.assertEqual(source.count(needle), 1)
        namespace = dict(vars(ib) if module is ib.LiveIB else vars(module))
        exec(compile(source.replace(needle, replacement), f"<inverse-{name}>", "exec"), namespace)
        return namespace[name]

    def test_variant_only_mismatch_at_choke(self):
        ctx = self.context()
        def call():
            request = bridge.bar_request("ibkr.gap_fill.session_fetch", contract(), "1d",
                                         datetime(2026, 7, 8, 16), "1 D")
            with bridge.send_scope(request, bridge.acquire_turn):
                # These are exactly the shared fields: only the variant differs.
                with self.assertRaises(RequestRefused):
                    bridge.take("ibkr-head", contract(), what_to_show="TRADES", use_rth=True)
            with bridge.send_scope(request, bridge.acquire_turn):
                with self.assertRaises(RequestRefused):
                    self.adapter.head_timestamp(contract())
        self.drive(ctx, call)
        self.assertEqual(self.adapter.ib.calls, [])
        self.assertEqual(self.pacer.turns, 0)

    def test_worker_scope_direct_callback_cannot_borrow_authority(self):
        ctx = self.context()
        @bridge.worker_scope
        def fixture(progress, pacer=None):
            self.assertIsNotNone(bridge.current_worker())
            progress()
        def callback():
            with self.assertRaises(RequestRefused):
                ib._probe_earliest_daily(self.adapter, contract(), date(2026, 7, 8))
            self.assertIsNone(bridge.current_worker())
        fixture(callback, pacer=self.pacer, _fetch_worker=ctx.worker("callback-only"))
        self.assertEqual(self.adapter.ib.calls, [])

    def test_direct_prefetch_refuses_future_empty_cache(self):
        ctx = self.context("2026-07-08T00:05:00")
        self.adapter.ib.rows = [bar(datetime(2026, 7, 7, 9, 30, tzinfo=NY))]
        result = {"requests": 0, "counters": {"invalid": 0, "outside_day": 0, "non_rth": 0}}
        with self.assertRaisesRegex(RequestRefused, "future empty session"):
            self.drive(ctx, lambda: ib._prefetch_unfiltered(self.adapter, self.pacer,
                contract(), "TEST", "1m", ["1m"], [date(2026, 7, 7), date(2026, 7, 8)],
                None, lambda _: None, result))

    def test_four_missing_guard_inverses_turn_pins_red(self):
        cases = [
            (bridge, "take", " or attempt.request.envelope.variant != variant", "",
             self.test_variant_only_mismatch_at_choke),
            (bridge, "take", "_SEND.set(None)", "pass",
             self.test_scope_consumed_before_observer_can_reenter),
            (bridge, "worker_scope", "without_authority(guard_callbacks)(bound.arguments)", "pass",
             self.test_worker_scope_direct_callback_cannot_borrow_authority),
            (ib, "_prefetch_unfiltered", "if fib.unsettled_days(iv, [d]) and not by_day.get(d):", "if False:",
             self.test_direct_prefetch_refuses_future_empty_cache),
        ]
        for module, name, needle, replacement, oracle in cases:
            with self.subTest(guard=name, needle=needle):
                mutant = self._mutant(module, name, needle, replacement)
                self.adapter.ib = RawIB()
                self.pacer.wait_turn = InstantPacer().wait_turn
                if needle == "_SEND.set(None)":
                    # The new attempt lock independently stops reentry. Preserve
                    # that redundancy explicitly, then remove both layers to
                    # prove this original observer oracle still detects a send.
                    with patch.object(module, name, mutant):
                        oracle()
                    self.adapter.ib = RawIB()
                    self.pacer.wait_turn = InstantPacer().wait_turn
                    with patch.object(module, name, mutant), patch.object(
                            LogicalRequest, "execute", LogicalRequest._execute), self.assertRaises(AssertionError):
                        oracle()
                    continue
                with patch.object(module, name, mutant), self.assertRaises(AssertionError):
                    oracle()

    def _planner_store_pin(self, interval, clock, pipeline, *, deferred=True):
        path = self.seed_daily(interval)
        ctx = self.context(clock)
        raw = RawIB([bar(date(2026, 7, d) if interval == "1d"
                         else datetime(2026, 7, d, 9, 30, tzinfo=NY)) for d in (7, 8, 9, 10)])
        self.adapter.ib = raw
        with patch.object(FetchRunContext, "create", return_value=ctx):
            report = ib.nightly_gap_fill_parallel(self.root, [("TEST", interval)], [1001],
                adapter_factory=lambda *a: lambda: self.adapter, pipeline=pipeline)
        res = report["series"][0]
        stored, _ = ib.ss.read_month_file(path)
        self.assertIsNone(res["halt"], res)
        self.assertGreater(res["written"], 0)
        expected = {date(2026, 7, 7), date(2026, 7, 8)}
        if not deferred:
            expected.add(date(2026, 7, 9))
            if datetime.fromisoformat(clock).date() == date(2026, 7, 11):
                expected.add(date(2026, 7, 10))
        self.assertEqual({b[0].date() for b in stored}, expected)
        self.assertEqual(res.get("horizon_deferred_days", []), ["2026-07-09"] if deferred else [])
        self.assertNotIn(date(2026, 7, 9), res.get("_empty_days", []))
        self.assertTrue(report["fetch_ledger"]["verified"])
        if deferred:
            # A partial intraday horizon must not re-admit today's whole session.
            self.assertTrue(all(c[2]["endDateTime"].date() <= date(2026, 7, 8)
                                for c in raw.calls if c[0] == "bars"))

    def test_real_planner_presettle_commits_settled_work_both_modes(self):
        for interval in ("1d", "1m"):
            for clock in ("00:05:00", "08:00:00", "13:20:00"):
                for pipeline in (False, True):
                    with self.subTest(interval=interval, clock=clock, pipeline=pipeline):
                        self._planner_store_pin(interval, "2026-07-09T" + clock, pipeline)

    def test_real_planner_after_settle_and_weekend_controls(self):
        # Saturday July 11: Friday July 10 is settled and must be committed.
        for clock in ("2026-07-09T17:00:00", "2026-07-11T08:00:00"):
            for interval in ("1d", "1m"):
                for pipeline in (False, True):
                    with self.subTest(clock=clock, interval=interval, pipeline=pipeline):
                        self._planner_store_pin(interval, clock, pipeline, deferred=False)

    def test_inverse_unsettled_day_readmission_breaks_real_store_pin(self):
        for interval in ("1d", "1m"):
            for pipeline in (False, True):
                with self.subTest(interval=interval, pipeline=pipeline):
                    with patch.object(ib, "_settled_plan_days", side_effect=lambda iv, days, res: list(days)):
                        with self.assertRaises(AssertionError):
                            self._planner_store_pin(interval, "2026-07-09T00:05:00", pipeline)

    def test_unexpected_failure_retains_partial_commit_counters(self):
        def failed(res, *args):
            res.update(written=2, added=50, requests=3, committed_through="2026-07-08")
            raise RuntimeError("injected failure after commits")
        with patch.object(ib, "_fill_series_inner", side_effect=failed):
            report = ib.nightly_gap_fill(self.root, [("TEST", "1d")],
                adapter_factory=lambda: self.adapter, pacer=self.pacer)
        res = report["series"][0]
        self.assertEqual((res["written"], res["added"], res["requests"]), (2, 50, 3))
        self.assertEqual(res["committed_through"], "2026-07-08")
        self.assertIn("injected failure", res["halt"])

    def test_unknown_ib_error_is_ledger_error_never_store_absence(self):
        for code in (321, 99999):
            with self.subTest(code=code):
                path = self.seed_daily()
                before = path.read_bytes()
                ctx = self.context("2026-07-09T00:05:00")
                raw = RawIB()
                self.adapter.ib = raw
                def rejected(c, **kwargs):
                    raw.calls.append(("bars", c.conId, kwargs))
                    self.adapter.errors.extend([(2106, "farm connected"), (code, "rejected duration")])
                    return []
                raw.reqHistoricalDataAsync = rejected
                with patch.object(FetchRunContext, "create", return_value=ctx):
                    report = ib.nightly_gap_fill_parallel(self.root, [("TEST", "1d")], [1001],
                        adapter_factory=lambda *a: lambda: self.adapter)
                res = report["series"][0]
                self.assertIn(str(code), res["halt"])
                self.assertEqual(path.read_bytes(), before)
                self.assertEqual(res["_empty_days"], [])
                self.assertEqual(res["written"], 0)
                results = [event["payload"] for event in inspect_ledger(ctx.ledger.path)["events"]
                           if event["event"] == "result" and event["producer_id"] == "ibkr.gap_fill.session_fetch"]
                self.assertEqual([result["outcome"] for result in results], ["error"])

    def test_documented_farm_notifications_are_not_request_errors(self):
        messages = {
            2105: "HMDS data farm connection is broken:ushmds",
            2119: "Market data farm is connecting:usfarm",
            1102: "Connectivity between IB and Trader Workstation has been restored - data maintained.",
            2168: "Etrade Only flag is not supported.",
        }
        for code in (*range(1100, 1103), *range(2100, 2200)):
            with self.subTest(code=code):
                self.seed_daily()
                ctx = self.context("2026-07-09T00:05:00")
                self.adapter.ib = RawIB()
                def response(c, **kwargs):
                    self.adapter.ib.calls.append(("bars", c.conId, kwargs))
                    self.adapter._on_error(-1, code, messages.get(code, "system/farm status"))
                    return [bar(date(2026, 7, 8))]
                self.adapter.ib.reqHistoricalDataAsync = response
                with patch.object(FetchRunContext, "create", return_value=ctx):
                    report = ib.nightly_gap_fill_parallel(self.root, [("TEST", "1d")], [1001],
                        adapter_factory=lambda *a: lambda: self.adapter)
                res = report["series"][0]
                self.assertIsNone(res["halt"], res)
                self.assertGreater(res["written"], 0)
                self.assertFalse((self.root / "_halted_series.json").exists())
                outcomes = [e["payload"]["outcome"] for e in inspect_ledger(ctx.ledger.path)["events"]
                            if e["event"] == "result" and e["producer_id"] == "ibkr.gap_fill.session_fetch"]
                self.assertEqual(outcomes, ["returned"])

    def _no_data_response(self, c, **kwargs):
        self.adapter.ib.calls.append(("bars", c.conId, kwargs))
        self.adapter._on_error(42, 162,
            "Historical Market Data Service error message:HMDS query returned no data: TEST@SMART Trades")
        return []

    def test_162_no_data_subminute_window_is_clean_empty(self):
        ctx = self.context()
        self.adapter.ib.reqHistoricalDataAsync = self._no_data_response
        result = self.drive(ctx, lambda: self.fetch_session("1s"))
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0][1], [])
        outcomes = [e["payload"]["outcome"] for e in self.events(ctx) if e["event"] == "result"]
        self.assertGreater(len(outcomes), 1)
        self.assertEqual(set(outcomes), {"empty"})
        self.assertFalse((self.root / "_halted_series.json").exists())

    def _targeted_empty_pre_month_case(self):
        raise AssertionError("targeted legacy case requires the confined Operations fixture")

    def test_empty_pre_month_without_control_is_unfilled_not_source_absent(self):
        # A2 targeted fill requires an explicit operation child. Keep this old
        # entry truthful by running both variants under the existing owner;
        # the six original assertions live in the shared oracle above.
        result = unittest.TestResult()
        unittest.TestSuite([self._targeted_empty_pre_month_case()]).run(result)
        self.assertEqual(result.testsRun, 1)
        self.assertTrue(result.wasSuccessful(), result.errors + result.failures)

    def test_162_empty_subwindow_does_not_abort_nonempty_day_commit(self):
        self.seed_daily("1s")
        ctx = self.context("2026-07-09T00:05:00")
        self.adapter.ib = raw = RawIB()
        def response(c, **kwargs):
            if kwargs["endDateTime"].time().replace(tzinfo=None) == time(10, 30):
                return self._no_data_response(c, **kwargs)
            raw.calls.append(("bars", c.conId, kwargs))
            return [bar(datetime(2026, 7, d, 9, 30, tzinfo=NY)) for d in (7, 8)]
        raw.reqHistoricalDataAsync = response
        with patch.object(FetchRunContext, "create", return_value=ctx):
            report = ib.nightly_gap_fill_parallel(self.root, [("TEST", "1s")], [1001],
                adapter_factory=lambda *a: lambda: self.adapter)
        res = report["series"][0]
        self.assertIsNone(res["halt"], res)
        self.assertGreater(res["written"], 0)
        self.assertEqual(res.get("_empty_days", []), [])
        self.assertFalse((self.root / "_halted_series.json").exists())
        outcomes = {e["payload"]["outcome"] for e in inspect_ledger(ctx.ledger.path)["events"]
                    if e["event"] == "result" and e["producer_id"] == "ibkr.gap_fill.session_fetch"}
        self.assertEqual(outcomes, {"empty", "returned"})

    def test_combined_prefetch_removes_holiday_from_each_token_cache(self):
        self.seed_daily("1m", date(2026, 9, 4))
        ctx = self.context("2026-09-08T00:05:00")
        days = [date(2026, 9, 4), date(2026, 9, 7)]
        tokens = ["1m", "1m-pre", "1m-post"]
        self.adapter.ib.rows = [bar(datetime(2026, 9, d, hour, minute, tzinfo=NY))
            for d in (4, 7) for hour, minute in ((4, 0), (9, 30), (16, 0))]
        # A look-ahead plan made outside the worker still needs token filtering.
        with patch.object(ib, "plan_gap", return_value={"days": days, "empty_series": False}):
            cache = self.drive(ctx, lambda: ib._prefetch_combined(self.root,
                self.adapter, self.pacer, "TEST", "1m", tokens, {"TEST": 123},
                date(2026, 9, 8), None, None, lambda _: None, {}))
        self.assertEqual({token: set(rows) for token, rows in cache.items()},
                         {token: {days[0]} for token in tokens})
        self.assertTrue(all(cache[token][days[0]] for token in tokens))
        for event in self.events(ctx):
            if event["event"] == "decision":
                self.assertEqual(datetime.fromisoformat(
                    event["payload"]["requested"]["intended_end"]).date(), days[0])

    def test_actual_holiday_refusal_persists_halt_if_plan_filter_is_bypassed(self):
        self.seed_daily("1s", date(2026, 9, 4))
        ctx = self.context("2026-09-08T00:05:00")
        self.adapter.ib.rows = [bar(datetime(2026, 9, 4, 9, 30, tzinfo=NY))]
        # Exercise the real envelope refusal as a second line of defense.
        with patch.object(ib, "_settled_plan_days", side_effect=lambda iv, days, res: list(days)), \
                patch.object(FetchRunContext, "create", return_value=ctx):
            report = ib.nightly_gap_fill_parallel(self.root, [("TEST", "1s")], [1001],
                adapter_factory=lambda *a: lambda: self.adapter)
        res = report["series"][0]
        self.assertEqual(res["halt_kind"], "request_refused")
        self.assertGreater(res["requests"], 1)
        self.assertIn(res["halt"], (self.root / "_halted_series.json").read_text())
        decisions = [e for e in inspect_ledger(ctx.ledger.path)["events"] if e["event"] == "decision"]
        self.assertEqual(decisions[-1]["payload"]["decision"], "refused")

    def test_no_data_does_not_mask_permission_contract_pacing_or_unknown(self):
        cases = [(354, "not subscribed", ib.SeriesHalt), (200, "No security definition", ib.SeriesHalt),
                 (162, "No market data permissions for TEST", ib.SeriesHalt),
                 (162, "Historical data request pacing violation", ib.PacingViolation),
                 (420, "pacing", ib.PacingViolation), (321, "invalid request", ib.SeriesHalt),
                 (99999, "unknown rejection", ib.SeriesHalt)]
        # Exercise transport once (no production retry loop) to pin precedence.
        for code, message, error in cases:
            with self.subTest(code=code, message=message):
                ctx = self.context()
                self.adapter.ib = RawIB()
                def response(c, **kwargs):
                    self._no_data_response(c, **kwargs)
                    self.adapter._on_error(42, code, message)
                    return []
                self.adapter.ib.reqHistoricalDataAsync = response
                def call():
                    request = bridge.bar_request("ibkr.gap_fill.session_fetch", contract(), "1d",
                                                 datetime(2026, 7, 8, 16), "1 D")
                    with bridge.send_scope(request, bridge.acquire_turn):
                        return self.adapter.fetch(contract(), datetime(2026, 7, 8, 16), "1 D", "1 day")
                with self.assertRaises(error):
                    self.drive(ctx, call)
                self.assertEqual(self.events(ctx)[1]["payload"]["outcome"], "error")

    def _holiday_store_pin(self, interval, clock, pipeline, *, holiday_only=False):
        path = self.seed_daily(interval, date(2026, 9, 4))
        before = path.read_bytes()
        ctx = self.context(clock)
        hour, minute = (16, 0) if interval.endswith("-post") else (9, 30)
        self.adapter.ib = RawIB([bar(date(2026, 9, d) if interval == "1d" else
            datetime(2026, 9, d, hour, minute, tzinfo=NY)) for d in (4, 7, 8)])
        raw = self.adapter.ib
        plan = ib.plan_gap(self.root, "TEST", interval, today=datetime.fromisoformat(clock).date())
        if holiday_only:
            plan["days"] = [date(2026, 9, 7)]
        with patch.object(FetchRunContext, "create", return_value=ctx), (
                patch.object(ib, "plan_gap", return_value=plan) if holiday_only else nullcontext()):
            report = ib.nightly_gap_fill_parallel(self.root, [("TEST", interval)], [1001],
                adapter_factory=lambda *a: lambda: self.adapter, pipeline=pipeline)
        res = report["series"][0]
        self.assertIsNone(res["halt"], res)
        self.assertEqual(res["calendar_closed_days"], ["2026-09-07"])
        self.assertNotIn(date(2026, 9, 7), res.get("_empty_days", []))
        self.assertFalse((self.root / "_halted_series.json").exists())
        stored, _ = ib.ss.read_month_file(path)
        self.assertEqual({b[0].date() for b in stored}, {date(2026, 9, 4)})
        sends = [c for c in raw.calls if c[0] == "bars"]
        if holiday_only:
            self.assertEqual(sends, [])
            self.assertEqual(path.read_bytes(), before)
            self.assertEqual(res["requests"], 0)
        else:
            self.assertTrue(sends)
            self.assertTrue(all(c[2]["endDateTime"].date() == date(2026, 9, 4) for c in sends))
        self.assertTrue(report["fetch_ledger"]["verified"])

    def test_holiday_real_planner_all_token_paths_both_modes(self):
        for interval in ("1s", "1m-post", "1m", "1d"):
            for clock in ("2026-09-07T12:00:00", "2026-09-08T00:05:00"):
                for pipeline in (False, True):
                    with self.subTest(interval=interval, clock=clock, pipeline=pipeline):
                        self._holiday_store_pin(interval, clock, pipeline)

    def test_holiday_only_coarse_chunk_never_sends(self):
        for interval in ("1m", "1d"):
            for pipeline in (False, True):
                with self.subTest(interval=interval, pipeline=pipeline):
                    self._holiday_store_pin(interval, "2026-09-08T00:05:00", pipeline, holiday_only=True)

    def test_request_refusal_is_durable_classified_and_preserves_counters(self):
        for written in (0, 2):
            with self.subTest(written=written):
                self.adapter.ib = RawIB()  # Prior operation disconnected its adapter.
                def refuse(res, *args):
                    res.update(written=written, added=50 if written else 0, requests=3)
                    raise RequestRefused("injected policy refusal")
                with patch.object(ib, "_fill_series_inner", side_effect=refuse):
                    result = ib.nightly_gap_fill(self.root, [("TEST", "1d")],
                        adapter_factory=lambda: self.adapter, pacer=self.pacer)["series"][0]
                self.assertEqual(result["halt_kind"], "request_refused")
                self.assertIn("injected policy refusal", result["halt"])
                self.assertEqual((result["written"], result["requests"]), (written, 3))
                self.assertIn("injected policy refusal", (self.root / "_halted_series.json").read_text())

    def test_month_partial_today_policy_differs_intentionally_from_session_plan(self):
        ctx = self.context("2026-07-09T13:20:00")
        self.adapter.ib.rows = [bar(datetime(2026, 7, 9, 9, 30, tzinfo=NY)),
                                bar(datetime(2026, 7, 9, 15, 59, tzinfo=NY))]
        rows, skipped = self.drive(ctx, lambda: ib._fetch_month_bars(
            self.adapter, contract(), 2026, 7, "1m"))
        self.assertEqual([r[0] for r in rows], [datetime(2026, 7, 9, 9, 30)])
        self.assertIn(date(2026, 7, 9), skipped)  # Day remains unknown despite settled prefix.
        res = {}
        planned = self.drive(ctx, lambda: ib._settled_plan_days("1m", [date(2026, 7, 9)], res))
        self.assertEqual(planned, [])  # Session updater admits complete sessions only.
        self.assertEqual(res["horizon_deferred_days"], ["2026-07-09"])

    def _early_close_store_pin(self, interval, half_day, clock, pipeline, *, mutated=False):
        previous = half_day - timedelta(days=2 if half_day.month == 11 else 1)
        path = self.seed_daily(interval, previous)
        authority = self.authority
        if mutated:
            rows = dict(authority.rows)
            rows[half_day] = replace(rows[half_day], status="open_early_close", close=time(13))
            authority = replace(authority, rows=rows)
        ctx = self.context(clock, authority=authority)
        base, _, session = parse_token(interval)
        times = {"pre": (time(4), time(9, 29)), "post": (time(16), time(19, 59)),
                 "rth": (time(9, 30), time(12, 59), time(14))}[session]
        raw = RawIB([bar(day if base == "1d" else datetime.combine(day, stamp, NY))
                     for day in (previous, half_day) for stamp in (times[:1] if base == "1d" else times)])
        self.adapter.ib = raw
        with patch.object(FetchRunContext, "create", return_value=ctx):
            report = ib.nightly_gap_fill_parallel(self.root, [("TEST", interval)], [1001],
                adapter_factory=lambda *a: lambda: self.adapter, pipeline=pipeline)
        res = report["series"][0]
        self.assertIsNone(res["halt"], res)
        self.assertTrue(report["fetch_ledger"]["verified"])
        self.assertFalse((self.root / "_halted_series.json").exists())
        excluded = session != "rth"
        daily_deferred = mutated and base == "1d"
        self.assertEqual(res.get("calendar_unsupported_days", []), [half_day.isoformat()] if excluded else [])
        self.assertNotIn(half_day, res.get("_empty_days", []))
        manifest = ib.ss.load_manifest(self.root / "TEST")
        section = manifest.get("intervals", {}).get(interval, {})
        self.assertNotIn(half_day.isoformat(), section.get("verified_absent", []))
        stored, _ = ib.ss.read_month_file(path)
        expected = {previous} if excluded or daily_deferred else {previous, half_day}
        self.assertEqual({row[0].date() for row in stored}, expected)
        if daily_deferred:
            self.assertIn(half_day.isoformat(), res.get("horizon_deferred_days", []))
        if not excluded and not daily_deferred:
            self.assertGreater(res["written"], 0)
            self.assertTrue(any(row[0].date() == half_day for row in stored))
        if base != "1d":
            self.assertTrue(all(row[0].time() < time(13) for row in stored if row[0].date() == half_day))
        if not mutated:
            holiday = date(2026, 11, 26) if half_day.month == 11 else date(2026, 12, 25)
            self.assertIn(holiday.isoformat(), res.get("calendar_closed_days", []))
        sends = [call[2] for call in raw.calls if call[0] == "bars"]
        half_sends = [call for call in sends if call["endDateTime"].date() == half_day]
        if excluded or daily_deferred:
            self.assertEqual(half_sends, [])
        else:
            self.assertTrue(half_sends)
            self.assertTrue(all(call["endDateTime"].time().replace(tzinfo=None) <= time(13) for call in half_sends))
        if base in ("1s", "5s"):
            window_seconds = ib._BAR_SIZES[base][1]
            self.assertEqual(len(half_sends), (210 * 60 + window_seconds - 1) // window_seconds)

    def test_real_early_close_planner_store_all_requested_tokens_both_modes(self):
        for half_day, clock in ((date(2026, 11, 27), "2026-11-30T00:05:00"),
                                (date(2026, 12, 24), "2026-12-28T00:05:00")):
            for interval in ("1s", "5s", "1m-pre", "1m-post", "1m", "1d"):
                for pipeline in (False, True):
                    with self.subTest(day=half_day, interval=interval, pipeline=pipeline):
                        self._early_close_store_pin(interval, half_day, clock, pipeline)

    def test_mutated_13_close_at_1320_keeps_daily_floor_and_intraday_commit(self):
        for interval in ("1s", "5s", "1m-pre", "1m-post", "1m", "1d"):
            for pipeline in (False, True):
                with self.subTest(interval=interval, pipeline=pipeline):
                    self._early_close_store_pin(interval, date(2026, 7, 8),
                        "2026-07-08T13:20:00", pipeline, mutated=True)

    def test_unsupported_plan_dates_excluded_but_coverage_and_raw_guards_stay_strict(self):
        ctx = self.context("2026-11-30T00:05:00")
        day = date(2026, 11, 27)
        for token in TOKENS:
            with self.subTest(token=token):
                res = {}
                planned = self.drive(ctx, lambda: ib._settled_plan_days(token, [day], res))
                extended = parse_token(token)[2] != "rth"
                self.assertEqual(planned, [] if extended else [day])
                self.assertEqual(res.get("calendar_unsupported_days", []), [day.isoformat()] if extended else [])
        with self.assertRaises(CalendarUnsupported):
            self.drive(ctx, lambda: ib._settled_plan_days("1m-pre", [date(1979, 12, 31)], {}))
        with self.assertRaises(CalendarUnsupported):
            ctx.authority.window("1m-pre", day)
        self.assertEqual(self.adapter.ib.calls, [])

    def test_combined_prefetch_excludes_unsupported_halfday_per_token(self):
        self.seed_daily("1m", date(2026, 11, 25))
        ctx = self.context("2026-11-30T00:05:00")
        before, holiday, half = date(2026, 11, 25), date(2026, 11, 26), date(2026, 11, 27)
        tokens = ["1m", "1m-pre", "1m-post"]
        self.adapter.ib.rows = [bar(datetime.combine(day, stamp, NY))
            for day in (before, half) for stamp in (time(4), time(9, 30), time(16))]
        with patch.object(ib, "plan_gap", return_value={"days": [before, holiday, half], "empty_series": False}):
            cache = self.drive(ctx, lambda: ib._prefetch_combined(self.root, self.adapter,
                self.pacer, "TEST", "1m", tokens, {"TEST": 123}, date(2026, 11, 30),
                None, None, lambda _: None, {}))
        self.assertEqual({token: set(days) for token, days in cache.items()},
                         {"1m": {before, half}, "1m-pre": {before}, "1m-post": {before}})
        for event in self.events(ctx):
            if event["event"] == "decision":
                envelope = event["payload"]["effective"]
                self.assertIsNotNone(envelope)
                self.assertEqual(datetime.fromisoformat(envelope["raw_end"]).date(),
                                 half if envelope["token"] == "1m" else before)

    def test_inverse_halfday_bounds_and_exclusion_turn_store_pins_red(self):
        for name, needle, replacement, oracle in (
            ("_fetch_sessions", "session_window=window", "session_window=None",
             lambda: self._early_close_store_pin("1s", date(2026, 11, 27), "2026-11-30T00:05:00", False)),
            ("_settled_plan_days", "unsupported.add(day)", "raise",
             lambda: self._early_close_store_pin("1m-post", date(2026, 11, 27), "2026-11-30T00:05:00", False)),
        ):
            with self.subTest(guard=name):
                mutant = self._mutant(ib, name, needle, replacement)
                with patch.object(ib, name, mutant), self.assertRaises(AssertionError):
                    oracle()

    def test_unknown_ib_error_cannot_publish_prefetch_cache(self):
        ctx = self.context()
        def rejected(*a, **k):
            self.adapter.errors.append((321, "rejected duration"))
            return []
        self.adapter.ib.reqHistoricalDataAsync = rejected
        res = {"requests": 0, "counters": {"invalid": 0, "outside_day": 0, "non_rth": 0}}
        with self.assertRaisesRegex(ib.SeriesHalt, "321"):
            self.drive(ctx, lambda: ib._prefetch_unfiltered(self.adapter, self.pacer,
                contract(), "TEST", "1m", ["1m"], [date(2026, 7, 8)],
                None, lambda _: None, res))
        self.assertEqual(self.events(ctx)[1]["payload"]["outcome"], "error")

    def test_inverse_carrier_and_unknown_error_guards_turn_pins_red(self):
        import fetch_envelopes
        from fetch_authority import UTC
        def old_seconds(start, end):
            return f"{int((end.astimezone(UTC) - start.astimezone(UTC)).total_seconds())} S"
        with patch.object(fetch_envelopes, "encode_carrier", side_effect=old_seconds):
            with self.assertRaises(AssertionError):
                self.test_all_bar_producers_effective_duration_grammar()
        self.adapter.ib = RawIB()
        with patch.object(fetch_envelopes, "encode_carrier",
                          side_effect=lambda start, end: f"{(end.date() - start.date()).days + 1} D"):
            with self.assertRaises(AssertionError):
                self.test_all_bar_producers_effective_duration_grammar()
        mutant = self._mutant(ib.LiveIB, "fetch",
            'raise SeriesHalt(f"IB historical request error {code}: {msg}")', "pass")
        self.adapter.ib = RawIB()
        with patch.object(ib.LiveIB, "fetch", mutant), self.assertRaises(AssertionError):
            self.test_unknown_ib_error_cannot_publish_prefetch_cache()

    def test_combined_prefetch_defers_per_token_before_sending(self):
        self.seed_daily("1m")
        ctx = self.context("2026-07-09T13:20:00")
        days = [date(2026, 7, 8), date(2026, 7, 9)]
        tokens = ["1m", "1m-pre", "1m-post"]
        self.adapter.ib.rows = [bar(datetime(2026, 7, d, hour, minute, tzinfo=NY))
                                for d in (8, 9) for hour, minute in ((4, 0), (9, 30), (16, 0))]
        # Planning is deliberately precomputed outside worker scope, as in
        # fleet look-ahead. Each token must still be partitioned at execution.
        plan = {"days": days, "empty_series": False}
        with patch.object(ib, "plan_gap", return_value=plan):
            cache = self.drive(ctx, lambda: ib._prefetch_combined(self.root,
                self.adapter, self.pacer, "TEST", "1m", tokens, {"TEST": 123},
                date(2026, 7, 9), None, None, lambda _: None, {}))
        self.assertEqual(set(cache["1m"]), {days[0]})
        self.assertEqual(set(cache["1m-post"]), {days[0]})
        self.assertEqual(set(cache["1m-pre"]), set(days))
        self.assertTrue(cache["1m-pre"][days[1]])
        events = self.events(ctx)
        for event in events:
            if event["event"] != "decision":
                continue
            requested = event["payload"]["requested"]
            expected = days[1] if requested["token"] == "1m-pre" else days[0]
            self.assertEqual(datetime.fromisoformat(requested["intended_end"]).date(), expected)

    def test_empty_series_presettle_plan_commits_history(self):
        for interval in ("1d", "1m"):
            with self.subTest(interval=interval):
                # Separate root per case: no existing interval to seed the plan.
                ctx = self.context("2026-07-09T00:05:00")
                bank = self.root / interval
                self.adapter.ib = RawIB()
                def response(c, **kwargs):
                    self.adapter.ib.calls.append(("bars", c.conId, kwargs))
                    stamp = (date(2026, 7, 8) if kwargs["barSizeSetting"] == "1 day"
                             else datetime(2026, 7, 8, 9, 30, tzinfo=NY))
                    return [bar(stamp)]
                self.adapter.ib.reqHistoricalDataAsync = response
                with patch.object(FetchRunContext, "create", return_value=ctx):
                    report = ib.nightly_gap_fill_parallel(bank, [("TEST", interval)], [1001],
                        adapter_factory=lambda *a: lambda: self.adapter, since=date(2026, 7, 8))
                res = report["series"][0]
                self.assertIsNone(res["halt"], res)
                self.assertGreater(res["written"], 0)
                self.assertEqual(res["horizon_deferred_days"], ["2026-07-09"])
                self.assertEqual(res.get("_empty_days", []), [])

    def test_intraday_month_presettle_preserves_settled_prefix(self):
        ctx = self.context("2026-07-09T00:05:00")
        self.adapter.ib.rows = [bar(datetime(2026, 7, d, 9, 30, tzinfo=NY)) for d in (8, 9)]
        rows, skipped = self.drive(ctx, lambda: ib._fetch_month_bars(
            self.adapter, contract(), 2026, 7, "1m"))
        self.assertEqual([b[0].date() for b in rows], [date(2026, 7, 8)])
        self.assertIn(date(2026, 7, 9), skipped)
        self.assertNotIn(date(2026, 7, 8), skipped)

    def test_all_bar_producers_effective_duration_grammar(self):
        ctx = self.context("2026-07-09T13:20:00")
        self.adapter.ib.rows = [bar(date(2026, 7, 8))]
        self.drive(ctx, lambda: self.fetch_session("1d", date(2026, 7, 8)))
        self.drive(ctx, lambda: ib._fetch_month_bars(self.adapter, contract(), 2026, 7, "1d"))
        self.drive(ctx, lambda: ib._probe_earliest_daily(self.adapter, contract(), date(2026, 7, 9)))
        # Intraday month producer is a distinct registered producer ID.
        self.adapter.ib.rows = [bar(datetime(2026, 7, 8, 9, 30, tzinfo=NY))]
        self.drive(ctx, lambda: ib._fetch_month_bars(self.adapter, contract(), 2026, 7, "1m"))
        events = self.events(ctx)
        decisions = [e for e in events if e["event"] == "decision"]
        self.assertEqual({e["producer_id"] for e in decisions}, {
            "ibkr.gap_fill.session_fetch", "ibkr.gap_fill.month_daily",
            "ibkr.gap_fill.month_intraday", "ibkr.gap_fill.head_daily_probe"})
        from fetch_envelopes import carrier_start
        for event in decisions:
            effective = event["payload"]["effective"]
            if effective is None:
                continue
            duration = effective["duration"]
            self.assertTrue(not duration.endswith(" S") or int(duration[:-2]) <= 28800)
            self.assertTrue(not duration.endswith(" D") or int(duration[:-2]) <= 365)
            if event["producer_id"] == "ibkr.gap_fill.head_daily_probe":
                self.assertTrue(duration.endswith(" Y"))
            self.assertLessEqual(carrier_start(datetime.fromisoformat(effective["raw_end"]), duration),
                                 datetime.fromisoformat(effective["intended_start"]))
        self.assertEqual([c[2]["durationStr"] for c in self.adapter.ib.calls if c[0] == "bars"],
                         [e["payload"]["effective"]["duration"] for e in decisions if e["payload"]["effective"]])

    def _gui_serial_dispatch_pin(self, inverse=False):
        # Execute only the actual serial dispatch statements with injected UI
        # callbacks; no display_data import, GUI, preflight socket or thread.
        tree = ast.parse((Path(ib.__file__).parent.parent / "display_data.py").read_text(encoding="utf-8"))
        start = next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == "_storage_ibkr_start")
        worker = next(n for n in ast.walk(start) if isinstance(n, ast.FunctionDef) and n.name == "_work")
        serial = next(i for i, n in enumerate(worker.body) if isinstance(n, ast.Assign)
                      and any(isinstance(t, ast.Name) and t.id == "serial_ports" for t in n.targets))
        body = ast.Module(body=worker.body[serial:], type_ignores=[])
        if inverse:
            dispatches = [n for n in ast.walk(body) if isinstance(n, ast.Attribute)
                          and n.attr == "nightly_update"]
            self.assertEqual(len(dispatches), 1)
            dispatches[0].attr = "gap_fill"
        calls = []
        ui = SimpleNamespace(_storage_root=self.root, _ibkr_port_status_cb=lambda *a: None,
                             _xval_on_series=lambda *a: None,
                             _fleet_callbacks=lambda *a, **k: (lambda *a: True, lambda *a: True))
        with patch.object(ib, "nightly_update", side_effect=lambda *a, **k: calls.append((a, k)) or {}), patch.object(ib, "gap_fill", return_value={}):
            exec(compile(body, "<serial-update-dispatch>", "exec"), {
                "ports": [1001, 1002], "self": ui, "say": lambda *a: None, "ev": None,
                "pev": None, "selections": [("TEST", "1d")], "update_since": None,
                "factory": lambda: None, "stock_ibkr": ib, "q": SimpleNamespace(put=lambda *a: None)})
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][0][2], [1001])
        self.assertFalse(calls[0][1]["adaptive"])

    def test_gui_serial_update_routes_through_a1_orchestrator(self):
        self._gui_serial_dispatch_pin()

    def test_inverse_gui_serial_direct_gap_fill_fails_routing_pin(self):
        with self.assertRaises(AssertionError):
            self._gui_serial_dispatch_pin(inverse=True)

    def test_inverse_notification_no_data_and_calendar_filters_turn_pins_red(self):
        cases = [
            (ib.LiveIB, "fetch", "if 1100 <= code <= 1102 or 2100 <= code <= 2199:", "if False:",
             "test_documented_farm_notifications_are_not_request_errors"),
            (ib.LiveIB, "fetch", "if code == 162 and not bars and re.search", "if False and re.search",
             "test_empty_pre_month_without_control_is_unfilled_not_source_absent"),
            (ib, "_settled_plan_days", "day not in deferred and day not in closed", "day not in deferred",
             "test_holiday_real_planner_all_token_paths_both_modes"),
        ]
        for module, name, needle, replacement, oracle in cases:
            with self.subTest(guard=needle):
                mutant = self._mutant(module, name, needle, replacement)
                # Independent cases capture subTest failures without leaking
                # their expected red results or halted sidecars into this case.
                result = unittest.TestResult()
                with patch.object(module, name, mutant):
                    Integration(oracle).run(result)
                self.assertEqual(result.errors, [])
                self.assertGreater(len(result.failures), 0)
