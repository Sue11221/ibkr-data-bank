"""Offline A2-3 HTTP label/range acceptance through the shipped executor."""

import ast
import base64
from datetime import date, datetime, timedelta, timezone
from contextlib import redirect_stdout
import hashlib
import inspect
import io
import json
from pathlib import Path
import re
import shutil
import tempfile
import textwrap
import threading
import unittest
import urllib.request
import ssl
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import Mock, patch
from email.message import Message
from urllib.error import HTTPError

import fetch_test_policy as tp

# Transport, process and GUI tripwires precede every production import.
with tp.offline_transports():
    from fetch_authority import (AuthorityError, NY, ResponseQuarantined,
                                 ScheduleAuthority, digest_value)
    from fetch_governors import GovernorRegistry
    from fetch_http_labels import (guarded_stockanalysis_attempt,
                                   stockanalysis_bounds, stockanalysis_url)
    import fetch_http_labels as labels
    from fetch_ledger import FetchLedger, LedgerError, inspect_ledger
    from fetch_run_context import (FetchRunContext, RequestCancelled,
                                   RequestRefused)
    import stock_validate
    import external_sweep
    import external_sweep_cli
    import fetch_send_inventory
    import fetch_http_transport as http_transport


# The physical site keys identify the legacy raw seams, which remain held;
# each witness reaches the guarded attempt through that exact owner instead.
# The provider dispatch is one logical edge, never a second ledger pair.
_A23A_BINDINGS = (
    ("http.stockanalysis.validation",
     "engine.stock_validate:fetch_daily_reference:http_urlopen:1",
     "stockanalysis.history", "http-series",
     "test_validation_helper_uses_guarded_worker_and_holds_warm_cache"),
    ("http.stockanalysis.external_sweep",
     "engine.external_sweep:StockAnalysisProvider._read:http_urlopen:1",
     "stockanalysis.history", "http-series",
     "test_sweep_provider_uses_same_guarded_attempt"),
    ("http.stockanalysis.provider_dispatch",
     "engine.external_sweep:_provider_fetch:provider_fetch:1",
     "stockanalysis.history", "logical",
     "test_sweep_provider_uses_same_guarded_attempt"),
)


class Clock:
    def __init__(self):
        self.value = 0.0

    def now(self):
        return self.value

    def sleep(self, seconds, cancel=None):
        self.value += seconds


def _source_inverse(function, old, new):
    """Mutate one inspected function in memory; never modify the source tree."""
    source = textwrap.dedent(inspect.getsource(function))
    if source.count(old) != 1:
        raise AssertionError("inverse anchor must match exactly once")
    namespace = function.__globals__
    missing = object()
    previous = namespace.get(function.__name__, missing)
    try:
        exec(compile(source.replace(old, new, 1), function.__code__.co_filename,
                     "exec"), namespace)
        return namespace[function.__name__]
    finally:
        if previous is missing:
            namespace.pop(function.__name__, None)
        else:
            namespace[function.__name__] = previous


class HttpLabels(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.authority = ScheduleAuthority.load()

    def setUp(self):
        self.guard = tp.offline_transports()
        self.guard.__enter__()
        self.addCleanup(self.guard.__exit__, None, None, None)
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.clock = Clock()
        self.governors = GovernorRegistry(
            time_fn=self.clock.now, sleep_fn=self.clock.sleep)
        stock_validate.clear_reference_cache()

    def context(self, now="2026-08-04T17:00:00", capability=None):
        context = FetchRunContext.create(
            Path(self.temp.name) / "ledgers", authority=self.authority,
            clock=lambda: datetime.fromisoformat(now).replace(tzinfo=NY),
            governors=self.governors, test_capability=capability)
        self.addCleanup(context.ledger.close)
        return context

    def execute(self, context, rows, *, rng="5Y"):
        bounds = stockanalysis_bounds(
            context.authority, context.captured_now, rng)
        request = context.worker("stockanalysis").request(
            "http.stockanalysis.validation", {
                "variant": "http-series", "endpoint": "stockanalysis.history",
                "subject": "TEST", "token": "1d",
                "intended_start": bounds["intended_start"],
                "intended_end": bounds["intended_end"]})
        raw = json.dumps({"data": rows}, allow_nan=False).encode("utf-8")
        result = request.execute(
            lambda effective: raw, acquire_turn=lambda: 0,
            governor=self.governors.provider("stockanalysis.history"),
            normalizer=lambda response: json.loads(response),
            attempt_evidence={
                "url": stockanalysis_url("TEST", rng), "range": rng,
                "requested_start_date": bounds["requested_start_date"],
                "truncated_coverage": bounds["truncated_coverage"],
            })
        return result, raw

    @staticmethod
    def row(day, value=10):
        return {"t": day, "o": value, "h": value + 1,
                "l": value - 1, "c": value, "v": 100}

    @staticmethod
    def malformed_date_strings():
        return {
            "space-prefix": " 2026-08-03 not a date",
            "tab-prefix": "\t2026-08-03T99:99:99",
            "unicode-space-prefix": "\u30002026-08-03T25:61:00",
            "epoch-underscore": "1_754_179_200",
            "epoch-unicode": "1754179200".translate(str.maketrans(
                "0123456789", "\u0660\u0661\u0662\u0663\u0664\u0665\u0666\u0667\u0668\u0669")),
            "epoch-exponent": "1.7541792e9",
            "epoch-sign": "+1754179200",
            "epoch-leading-zero": "0001754179200",
            "epoch-fraction": "1754179200.999",
            "iso-trailing-space": "2026-08-03 ",
            "iso-trailing-junk": "2026-08-03 not a date",
            "epoch-surrounding-space": " 1754179200 ",
        }

    @staticmethod
    def malformed_shapes():
        base = b',"o":10,"h":11,"l":9,"c":10,"v":100}]}'
        return {
            "infinite-date": b'{"data":[{"t":1e400' + base,
            "huge-integer": b'{"data":[{"t":' + b'1' * 5001 + base,
            "deep-nesting": b'[' * 100000 + b'0' + b']' * 100000,
            "oversized-date": (b'{"data":[{"t":"2026-08-03T00:00:00.'
                               + b'0' * 42 + b'Z"' + base),
            **{name: json.dumps({"data": [HttpLabels.row(value)]}).encode()
               for name, value in HttpLabels.malformed_date_strings().items()},
        }

    def test_strict_string_dates_preserve_valid_iso_and_epoch_fields(self):
        epoch = int(datetime(2026, 8, 3, tzinfo=timezone.utc).timestamp())
        values = ("2026-08-03", "2026-08-03T23:59:59.123456789-04:00",
                  "2026-08-03T00:00:00Z", epoch, float(epoch), str(epoch),
                  epoch * 1000, str(epoch * 1000))
        for value in values:
            with self.subTest(value=value):
                row = labels.normalize_stockanalysis_rows([self.row(value)])[0]
                self.assertEqual(row["source_date"], value)
                self.assertIs(type(row["source_date"]), type(value))
                self.assertEqual(row["label"], "2026-08-03")

    def _assert_malformed_date_string(self, value):
        with self.assertRaisesRegex(ResponseQuarantined, "malformed StockAnalysis"):
            labels.normalize_stockanalysis_rows([self.row(value)])

    def test_strict_string_dates_refuse_lenient_forms(self):
        for name, value in self.malformed_date_strings().items():
            with self.subTest(name=name):
                self._assert_malformed_date_string(value)

    def _assert_escaped_date_boundary(self, value, expected_size):
        self.assertEqual(len(json.dumps(value, ensure_ascii=True)), expected_size)
        with self.assertRaisesRegex(ResponseQuarantined,
                "source date exceeds" if expected_size > 64 else
                "malformed StockAnalysis"):
            labels.normalize_stockanalysis_rows([self.row(value)])

    @staticmethod
    def escaped_date_boundaries():
        return {"quote": "2026-08-03" + '"' * 26,
                "backslash": "2026-08-03" + "\\" * 26,
                "bmp": "\u3000" * 8 + "2026-08-03" + " " * 4,
                "astral": "\U0001f600" * 4 + "2026-08-03" + " " * 4}

    def test_escaped_date_bound_is_checked_before_syntax(self):
        for name, value in self.escaped_date_boundaries().items():
            with self.subTest(name=name):
                self._assert_escaped_date_boundary(value, 64)
                self._assert_escaped_date_boundary(value + " ", 65)
        self._assert_escaped_date_boundary("\u3000" * 9 + "2026-08-03", 66)

    def test_strict_string_admission_and_escape_blind_inverses_are_red(self):
        strict = labels._normalize_stockanalysis_rows_strict
        lenient = _source_inverse(strict,
                                  "if isinstance(source_date, str):", "if False:")
        with patch.object(labels, "_normalize_stockanalysis_rows_strict", lenient):
            self.test_strict_string_dates_preserve_valid_iso_and_epoch_fields()
            for name, value in self.malformed_date_strings().items():
                with self.subTest(name=name):
                    with self.assertRaisesRegex(AssertionError,
                                                "ResponseQuarantined not raised"):
                        self._assert_malformed_date_string(value)
                    print("A37_STRING_INVERSE_RED", name)
        escape_blind = _source_inverse(strict, "ensure_ascii=True", "ensure_ascii=False")
        with patch.object(labels, "_normalize_stockanalysis_rows_strict", escape_blind):
            self.test_strict_string_dates_preserve_valid_iso_and_epoch_fields()
            for name in ("bmp", "astral"):
                with self.subTest(name=name):
                    with self.assertRaisesRegex(AssertionError, "does not match"):
                        self._assert_escaped_date_boundary(
                            self.escaped_date_boundaries()[name] + " ", 65)
                    print("A37_ESCAPE_INVERSE_RED", name)

    def test_max_and_five_year_bounds_are_covered_and_settled(self):
        context = self.context()
        maximum = stockanalysis_bounds(
            context.authority, context.captured_now, "Max")
        five = stockanalysis_bounds(
            context.authority, context.captured_now, "5Y")
        self.assertEqual(maximum["requested_start_date"],
                         context.authority.first_date.isoformat())
        self.assertEqual(maximum["intended_start"].date(),
                         context.authority.first_date)
        self.assertEqual(five["requested_start_date"], "2021-08-04")
        self.assertGreaterEqual(five["intended_start"].date(), date(2021, 8, 4))
        self.assertEqual(maximum["intended_end"], context.horizons["1d"])
        self.assertEqual(five["intended_end"], context.horizons["1d"])
        leap = self.context("2024-02-29T17:00:00")
        self.assertEqual(stockanalysis_bounds(
            leap.authority, leap.captured_now, "5Y")["requested_start_date"],
            "2019-02-28")
        with self.assertRaises(AuthorityError):
            stockanalysis_bounds(context.authority, context.captured_now, "All")

    def test_source_date_json_bound_preserves_rows_and_bounds_cache_size(self):
        # JSON quotes count toward the bound; accepted source text is never cut.
        longest = "2026-08-03T00:00:00." + "0" * 41 + "Z"
        self.assertEqual(len(json.dumps(longest)), 64)
        accepted = labels.normalize_stockanalysis_rows([self.row(longest)])
        self.assertEqual(accepted[0]["source_date"], longest)
        for value in (longest[:-1] + "0Z", "1" * 65, 10 ** 64):
            with self.subTest(source_type=type(value).__name__):
                with self.assertRaisesRegex(ResponseQuarantined,
                                            "source date exceeds"):
                    labels.normalize_stockanalysis_rows([self.row(value)])
        # Conservative encoded-byte proof, not a sampling of average rows:
        # exact fixed JSON punctuation/keys plus 64-byte source, 12-byte label,
        # 34-byte timestamp and five <=24-byte finite nonnegative floats.
        skeleton = {"source_date": None, "label": None, "timestamp": None,
                    **{key: None for key in ("o", "h", "l", "c", "v")}}
        fixed = len(json.dumps(skeleton, separators=(",", ":"))) - 8 * 4
        row_bound = fixed + 64 + 12 + 34 + 5 * 24
        reference_entry_bound = 12 + 1 + 24 + 1
        candidate_bound = (2 + labels._MAX_HISTORY_ROWS * (row_bound + 1)
                           + 2 + labels._MAX_HISTORY_ROWS * reference_entry_bound
                           + 65536)
        self.assertEqual(labels._MAX_HISTORY_ROWS, external_sweep.MAX_REFERENCE_ROWS)
        self.assertLess(candidate_bound, external_sweep.MAX_REFERENCE_BYTES)
        print(f"A36_CANDIDATE_BOUND bytes={candidate_bound} cap="
              f"{external_sweep.MAX_REFERENCE_BYTES} metadata_allowance=65536")

    def _oversized_sweep_case(self, *, single=False):
        import fetch_run_context as run_context

        project = Path(tempfile.mkdtemp(dir=self.temp.name,
            prefix="oversized-single-" if single else "oversized-batch-"))
        logs, bank = project / "Run Logs", project / "bank"
        cache = logs / "cache"
        cache.mkdir(parents=True, exist_ok=True)
        days = []
        for month_index in range(12):
            year = 2025 + (7 + month_index) // 12
            month = (7 + month_index) % 12 + 1
            day = date(year, month, 3)
            while self.authority.window("1d", day) is None:
                day += timedelta(days=1)
            days.append(day.isoformat())
        wire_rows = [self.row(day, 100) for day in days]
        good = json.dumps({"data": wire_rows}, separators=(",", ":")).encode()
        wire_rows[-1]["t"] += "T00:00:00.Z"
        base = json.dumps({"data": wire_rows}, separators=(",", ":")).encode()
        padding = labels._MAX_HISTORY_BYTES - 16 - len(base)
        wire_rows[-1]["t"] = wire_rows[-1]["t"][:-1] + "0" * padding + "Z"
        bad = json.dumps({"data": wire_rows}, separators=(",", ":")).encode()
        self.assertEqual(len(bad), labels._MAX_HISTORY_BYTES - 16)
        snapshots = {
            name: {"ticker": name, "conid": index, "provider_symbol": name,
                   "manifest_fingerprint": str(index) * 64,
                   "stored": {day: 100.0 for day in days},
                   "stored_rows": len(days), "stored_first": min(days),
                   "stored_last": max(days), "stored_basis": "raw",
                   "basis_actions_applied": []}
            for index, name in enumerate(("AAA", "BBB", "CCC"), 1)}
        previous = external_sweep._cache_path(cache, snapshots["BBB"])
        previous.write_bytes(b"last-good unchanged\n")
        sent = []
        def sender(url, _timeout):
            name = next(name for name in snapshots if f"/s/{name}/" in url)
            sent.append(name)
            return bad if name == "BBB" else good
        provider = external_sweep.StockAnalysisProvider(
            opener=sender, now=lambda: datetime(2026, 8, 4, tzinfo=timezone.utc))
        with (tp.offline_policy() as capability,
              patch.object(run_context, "PROJECT_ROOT", project),
              patch.object(external_sweep, "stored_daily_snapshot",
                           side_effect=lambda _root, name: snapshots[name]),
              patch.object(external_sweep, "_manifest_record",
                           side_effect=lambda _root, name: snapshots[name]),
              patch.object(external_sweep, "_manifest_unchanged", return_value=True)):
            common = dict(provider=provider, cache_root=cache,
                          _test_capability=capability,
                          _evidence_dir=logs / "ledgers", _authority=self.authority,
                          _clock=lambda: datetime(2026, 8, 4, 17, tzinfo=NY),
                          _governors=self.governors)
            if single:
                bad_row = external_sweep.sweep_ticker(bank, "BBB", **common)
            else:
                report = external_sweep.sweep_bank(
                    bank, list(snapshots), run_logs_root=logs,
                    gate_path=logs / ".sweep.lock", clock=self.clock.now,
                    sleep_fn=self.clock.sleep,
                    now=datetime(2026, 8, 4, tzinfo=timezone.utc), **common)
                bad_row = report["rows"][1]
                self.assertEqual(report["publication_state"], "complete")
                self.assertEqual(report["recovery_debt"], [])
                self.assertEqual(sent, ["AAA", "BBB", "CCC"])
                self.assertTrue(external_sweep._cache_path(cache, snapshots["AAA"]).is_file())
                self.assertTrue(external_sweep._cache_path(cache, snapshots["CCC"]).is_file())
            self.assertEqual(bad_row["error_type"], "ResponseQuarantined")
            self.assertEqual(bad_row["verdict"], "UNVERIFIABLE")
            self.assertEqual(bad_row["network_requests"], 1)
            self.assertEqual(previous.read_bytes(), b"last-good unchanged\n")
            audit = inspect_ledger(next((logs / "ledgers").glob("*.jsonl")))
            self.assertTrue(audit["verified"])
            self.assertEqual(len(audit["events"]), 3 if single else 7)
            quarantined = json.loads((project / bad_row["quarantine"]).read_text())
            self.assertEqual(base64.b64decode(
                quarantined["response"]["raw_response_base64"]), bad)
            self.assertFalse(list(logs.glob("external-sweep-spool-*")))

    def test_oversized_source_is_per_ticker_in_both_sweep_roots(self):
        self._oversized_sweep_case()
        self._oversized_sweep_case(single=True)

    def test_oversized_source_bound_inverses_reproduce_old_root_failures(self):
        with patch.object(labels, "_MAX_SOURCE_DATE_JSON_BYTES", 8 * 1024 * 1024):
            with self.assertRaisesRegex(external_sweep.SpoolError, "spool bound"):
                self._oversized_sweep_case()
            with self.assertRaises(external_sweep.PublicationRecoveryDebt) as caught:
                self._oversized_sweep_case(single=True)
            self.assertEqual(caught.exception.items, [
                {"ticker": "BBB", "stage": "cache", "cause_type": "EvidenceError"}])
        print("A36_SOURCE_BOUND_INVERSE_RED batch-abort single-publication-debt")

    def test_original_iso_and_epoch_labels_use_authority_close(self):
        # 00:30 UTC is still the previous day in New York. The source epoch
        # is a UTC calendar label, not a New York timestamp to shift.
        epoch = int(datetime(2026, 8, 4, 0, 30, tzinfo=timezone.utc).timestamp())
        with tp.offline_policy() as capability:
            context = self.context(capability=capability)
            accepted, raw = self.execute(context, [self.row("2026-08-03"),
                                                    self.row(epoch, 20)])
            context.seal()
        self.assertEqual([row["label"] for row in accepted],
                         ["2026-08-03", "2026-08-04"])
        self.assertEqual(accepted[1]["source_date"], epoch)
        self.assertEqual(accepted[1]["timestamp"], self.authority.window(
            "1d", date(2026, 8, 4))[1].astimezone(timezone.utc).isoformat())
        result = inspect_ledger(context.ledger.path)["events"][1]["payload"]
        self.assertEqual(result["raw_response_digest"], hashlib.sha256(raw).hexdigest())
        self.assertEqual((result["observed"]["count"],
                          result["accepted"]["count"], result["dropped"]),
                         (2, 2, 0))

    def test_default_context_holds_before_attempt(self):
        context = self.context()
        bounds = stockanalysis_bounds(
            context.authority, context.captured_now, "Max")
        with self.assertRaises(RequestRefused):
            context.worker("stockanalysis").request(
                "http.stockanalysis.validation", {
                    "variant": "http-series", "endpoint": "stockanalysis.history",
                    "subject": "TEST", "token": "1d",
                    "intended_start": bounds["intended_start"],
                    "intended_end": bounds["intended_end"]})
        context.seal()
        self.assertEqual(len(inspect_ledger(context.ledger.path)["events"]), 1)

    def test_endpoint_fixed_bridge_reaches_one_guarded_attempt(self):
        raw = json.dumps({"data": [self.row("2026-08-03")]}).encode("utf-8")
        with tp.offline_policy() as capability:
            context = self.context(capability=capability)
            sent = []
            result = guarded_stockanalysis_attempt(
                context.worker("stockanalysis"),
                "http.stockanalysis.validation", "BF.B", "5Y",
                lambda url: sent.append(url) or raw)
            context.seal()
        self.assertEqual(sent, [stockanalysis_url("BF.B", "5Y")])
        self.assertEqual(result[0]["label"], "2026-08-03")
        events = inspect_ledger(context.ledger.path)["events"]
        self.assertEqual([e["event"] for e in events],
                         ["decision", "result", "seal"])
        self.assertEqual(events[0]["producer_id"], "http.stockanalysis.validation")
        self.assertEqual(events[0]["payload"]["http_request"], {
            "url": stockanalysis_url("BF.B", "5Y"), "range": "5Y",
            "requested_start_date": "2021-08-04",
            "truncated_coverage": False,
        })
        self.assertEqual(events[1]["payload"]["accepted"]["count"], 1)
        for bad in ("BF/B", "BF.B@evil", ""):
            with self.assertRaises(AuthorityError):
                stockanalysis_url(bad, "5Y")

    def test_bridge_refuses_callback_without_offline_policy(self):
        context = self.context()
        sent = []
        with self.assertRaises(RequestRefused):
            guarded_stockanalysis_attempt(context.worker("stockanalysis"),
                "http.stockanalysis.validation", "TEST", "Max",
                lambda url: sent.append(url))
        self.assertEqual(sent, [])
        context.seal()

    def test_validation_helper_uses_guarded_worker_and_holds_warm_cache(self):
        raw = json.dumps({"data": [self.row("2026-08-03")]}).encode("utf-8")
        with tp.offline_policy() as capability:
            context = self.context(capability=capability)
            seen = []
            rows = stock_validate.fetch_daily_reference(
                "TEST", rng="Max", _fetch_worker=context.worker("validation"),
                _opener=lambda url: seen.append(url) or raw)
            context.seal()
        self.assertEqual(seen, [stockanalysis_url("TEST", "Max")])
        self.assertEqual(rows, {"2026-08-03": (10.0, 11.0, 9.0, 10.0, 100)})
        events = inspect_ledger(context.ledger.path)["events"]
        self.assertEqual([item["event"] for item in events],
                         ["decision", "result", "seal"])
        self.assertEqual(events[0]["producer_id"], "http.stockanalysis.validation")

        key = ("TEST", "Max")
        previous = stock_validate._REF_CACHE.get(key)
        stock_validate._REF_CACHE[key] = (10**15, rows)
        try:
            with self.assertRaises(RequestRefused):
                stock_validate.fetch_daily_reference("TEST", rng="Max")
        finally:
            if previous is None:
                stock_validate._REF_CACHE.pop(key, None)
            else:
                stock_validate._REF_CACHE[key] = previous

    def test_sweep_provider_uses_same_guarded_attempt(self):
        raw = json.dumps({"data": [self.row("2026-08-03")]}).encode("utf-8")
        with tp.offline_policy() as capability:
            context = self.context(capability=capability)
            sent = []
            provider = external_sweep.StockAnalysisProvider(
                opener=lambda url, timeout: sent.append((url, timeout)) or raw,
                now=lambda: datetime(2026, 8, 4, tzinfo=timezone.utc))
            result = external_sweep._provider_fetch(
                provider, "TEST", _fetch_worker=context.worker("external-sweep"))
            context.seal()
        self.assertEqual(sent, [(stockanalysis_url("TEST", "Max"), 20.0)])
        self.assertEqual(result["reference"], {"2026-08-03": 10.0})
        self.assertEqual(result["source_bytes_sha256"], hashlib.sha256(raw).hexdigest())
        events = inspect_ledger(context.ledger.path)["events"]
        self.assertEqual([item["event"] for item in events],
                         ["decision", "result", "seal"])
        self.assertEqual(events[0]["producer_id"],
                         "http.stockanalysis.external_sweep")
        self.assertEqual(events[0]["payload"]["http_request"]["range"], "Max")

        held = self.context()
        with self.assertRaises(RequestRefused):
            external_sweep._provider_fetch(
                provider, "TEST", _fetch_worker=held.worker("external-sweep"))
        held.seal()
        self.assertEqual(len(inspect_ledger(held.ledger.path)["events"]), 1)

        with tp.offline_policy() as capability:
            context = self.context(capability=capability)
            with self.assertRaises(RequestRefused):
                external_sweep._provider_fetch(
                    lambda symbol: raw, "TEST",
                    _fetch_worker=context.worker("external-sweep"))
            context.seal()

    def test_a23a_execution_binding_table_has_reached_pairs(self):
        producers = {row[0] for row in _A23A_BINDINGS}
        self.assertEqual(producers, {
            "http.stockanalysis.validation",
            "http.stockanalysis.external_sweep",
            "http.stockanalysis.provider_dispatch"})
        candidates = [row for row in fetch_send_inventory.build_inventory()["sites"]
                      if row["producer_id"] in producers]
        self.assertEqual(len(candidates), 3)
        sites = {row["producer_id"]: row for row in candidates}
        self.assertEqual(set(sites), producers)
        self.assertEqual(len({row["site_key"] for row in candidates}), 3)
        physical = producers - {"http.stockanalysis.provider_dispatch"}
        self.assertEqual({key: tp.TEST_PRODUCERS[key] for key in physical}, {
            key: ("http-series", "stockanalysis.history") for key in physical})
        owners = {
            "http.stockanalysis.validation":
                (("stock_validate.py", "fetch_daily_reference"),),
            "http.stockanalysis.external_sweep":
                (("external_sweep.py", "_read"),
                 ("external_sweep.py", "_provider_fetch")),
        }
        site_for = {row[0]: row[1] for row in _A23A_BINDINGS}
        reached, decisions, results = set(), set(), set()
        active_witness = None
        decision = FetchLedger.decision
        result = FetchLedger.result

        def record_decision(ledger, ids, payload):
            saved = decision(ledger, ids, payload)
            producer = ids["producer_id"]
            if producer in physical:
                frames = set()
                frame = inspect.currentframe().f_back
                while frame is not None:
                    frames.add((Path(frame.f_code.co_filename).name,
                                frame.f_code.co_name))
                    frame = frame.f_back
                required = owners[producer]
                self.assertTrue(all(owner in frames for owner in required))
                requested = payload["requested"]
                reached.add((producer, site_for[producer],
                             requested["endpoint"], requested["variant"],
                             active_witness))
                if producer == "http.stockanalysis.external_sweep":
                    reached.add(("http.stockanalysis.provider_dispatch",
                                 site_for["http.stockanalysis.provider_dispatch"],
                                 requested["endpoint"], "logical",
                                 active_witness))
                decisions.add((ids["operation_id"], ids["attempt_id"]))
            return saved

        def record_result(ledger, ids, payload):
            saved = result(ledger, ids, payload)
            if ids["producer_id"] in physical:
                results.add((ids["operation_id"], ids["attempt_id"]))
            return saved

        with (patch.object(FetchLedger, "decision", record_decision),
              patch.object(FetchLedger, "result", record_result)):
            for name in sorted({row[4] for row in _A23A_BINDINGS}):
                active_witness = name
                getattr(self, name)()

        def verify(bindings):
            assert len(bindings) == 3 and len({row[0] for row in bindings}) == 3
            assert {(row[0], row[1]) for row in bindings} == {
                (producer, site["site_key"]) for producer, site in sites.items()}
            assert all(sites[row[0]]["disposition"] == "a2_refused"
                       and sites[row[0]]["transport"] == "http-series"
                       and row[4] in dir(type(self)) for row in bindings)
            assert set(bindings) == reached
            assert decisions == results and len(decisions) == 2

        verify(_A23A_BINDINGS)
        for label, changed in (
                ("missing-logical", _A23A_BINDINGS[:2]),
                ("wrong-site", (_A23A_BINDINGS[0][:1] +
                                ("wrong-site",) + _A23A_BINDINGS[0][2:],)
                 + _A23A_BINDINGS[1:]),
                ("wrong-endpoint", (_A23A_BINDINGS[0][:2] +
                                    ("other.endpoint",) +
                                    _A23A_BINDINGS[0][3:],)
                 + _A23A_BINDINGS[1:]),
                ("wrong-witness", (_A23A_BINDINGS[0][:4] +
                                   (_A23A_BINDINGS[1][4],),)
                 + _A23A_BINDINGS[1:])):
            with self.subTest(mutant=label), self.assertRaises(AssertionError):
                verify(changed)
            print("A23A_BINDING_INVERSE_RED", label)

    def test_cross_transport_binding_denominator_reconciles_ibkr_and_http(self):
        source = (Path(__file__).parent /
                  "fetch_a2_ibkr_workflows_selftest.py").read_text(
                      encoding="utf-8")
        module = ast.parse(source)
        declared = [ast.literal_eval(node.value) for node in module.body
                    if isinstance(node, ast.Assign)
                    and any(isinstance(target, ast.Name)
                            and target.id == "_D01_BINDINGS"
                            for target in node.targets)]
        self.assertEqual(len(declared), 1)
        ibkr = declared[0]
        self.assertEqual(len(ibkr), 22)
        self.assertEqual(len({row[0] for row in ibkr}), 21)
        http = _A23A_BINDINGS
        self.assertEqual(len(http), 3)
        expected = {(row[0], row[1], row[3]) for row in ibkr} | {
            (row[0], row[1], row[3]) for row in http}
        self.assertEqual(len(expected), 25)
        producers = {row[0] for row in ibkr} | {row[0] for row in http}
        actual = {(site["producer_id"], site["site_key"],
                   "logical" if site["primitive"] == "provider_fetch"
                   else site["transport"])
                  for site in fetch_send_inventory.build_inventory()["sites"]
                  if site["producer_id"] in producers}
        self.assertEqual(actual, expected)
        self.assertEqual({row[0] for row in http[:2]},
                         {"http.stockanalysis.validation",
                          "http.stockanalysis.external_sweep"})
        self.assertTrue(all(site["disposition"] == "a2_refused"
                            for site in fetch_send_inventory.build_inventory()["sites"]
                            if site["producer_id"] in {
                                row[0] for row in http}))
        for name, mutant in (
                ("missing-http", expected - {(http[0][0], http[0][1],
                                             http[0][3])}),
                ("wrong-transport", (expected - {(http[1][0], http[1][1],
                                                   http[1][3])}) |
                 {(http[1][0], http[1][1], "ibkr-bars")})):
            with self.subTest(mutant=name), self.assertRaises(AssertionError):
                self.assertEqual(actual, mutant)
            print("A23A_CROSS_TRANSPORT_INVERSE_RED", name)

    def test_validation_registered_roots_hold_before_bank_or_sender(self):
        touched = []
        def read(*args):
            touched.append("bank")
            return []
        def send(url):
            touched.append("sender")
            return b"{}"
        directory = Path(self.temp.name) / "root-ledgers"
        entries = (
            lambda: stock_validate.fetch_daily_reference_single_request(
                "TEST", "Max", _evidence_dir=directory, _opener=send),
            lambda: stock_validate.validate_series(
                Path(self.temp.name) / "bank", "TEST", "1d",
                read_fn=read, _evidence_dir=directory, _opener=send),
            lambda: stock_validate.cross_validate_ticker(
                Path(self.temp.name) / "bank", "TEST", "1d",
                read_fn=read, _evidence_dir=directory, _opener=send),
            lambda: stock_validate.revalidate_library(
                Path(self.temp.name) / "bank", series=[("TEST", "1d")],
                read_fn=read, _evidence_dir=directory, _opener=send),
        )
        for entry in entries:
            with self.subTest(entry=entry), self.assertRaises(AuthorityError):
                entry()
        self.assertEqual(touched, [])

    def test_validation_roots_direct_positive_share_guarded_consumer(self, use_default=False):
        raw = json.dumps({"data": [self.row("2026-08-03")]}).encode("utf-8")
        bars = [(datetime(2026, 8, 3), 10.0, 11.0, 9.0, 10.0, 100)]
        seen = []
        def opener(url):
            seen.append(url)
            return raw
        responses = self.a55_default_receiver(opener) if use_default else []
        def fingerprint(_root, ticker, interval):
            return {"schema_version": stock_validate.ss.INTERVAL_FINGERPRINT_VERSION,
                    "algorithm": "sha256", "sha256": "a" * 64,
                    "ticker": ticker, "interval": interval,
                    "present": True, "backfill_incomplete": False,
                    "month_count": 1, "verified_absent_count": 0}
        common = {
            "_evidence_dir": Path(self.temp.name) / "validation-ledgers",
            "_authority": self.authority,
            "_clock": lambda: datetime(2026, 8, 4, 17, tzinfo=NY),
            "_governors": self.governors, "_opener": None if use_default else opener,
        }
        with tp.offline_policy() as capability:
            common["_test_capability"] = capability
            valid = stock_validate.validate_series(
                "unused-bank", "TEST", "1d", rng="Max",
                read_fn=lambda *_args: bars, **common)
            stock_validate.clear_reference_cache()
            cross = stock_validate.cross_validate_ticker(
                "unused-bank", "TEST", "1d", rng="Max",
                read_fn=lambda *_args: bars,
                fingerprint_fn=fingerprint, **common)
            stock_validate.clear_reference_cache()
            batch = stock_validate.revalidate_library(
                "unused-bank", series=[("TEST", "1d")], rng="Max",
                read_fn=lambda *_args: bars, **common)
        self.assertEqual(valid["checked"], 1)
        self.assertEqual(cross["status"], "validated")
        self.assertEqual(batch["summary"]["total"], 1)
        self.assertEqual(seen, [stockanalysis_url("TEST", "Max")] * 3)
        if use_default:
            self.assertEqual(len(responses), 3)
            self.assertTrue(all(response.closed for response in responses))
            ledgers = list(common["_evidence_dir"].glob("*.jsonl"))
            self.assertEqual(len(ledgers), 3)
            for path in ledgers:
                events = inspect_ledger(path)["events"]
                self.assertEqual([event["event"] for event in events],
                                 ["decision", "result", "seal"])
                self.assertEqual(events[0]["producer_id"], "http.stockanalysis.validation")

    def test_validation_wrappers_propagate_terminal_ledger_fault(self):
        bars = [(datetime(2026, 8, 3), 10.0, 11.0, 9.0, 10.0, 100)]
        directory = Path(self.temp.name) / "terminal-validation-ledgers"
        def fingerprint(_root, ticker, interval):
            return {
                "schema_version": stock_validate.ss.INTERVAL_FINGERPRINT_VERSION,
                "algorithm": "sha256", "sha256": "a" * 64,
                "ticker": ticker, "interval": interval, "present": True,
                "backfill_incomplete": False, "month_count": 1,
                "verified_absent_count": 0,
            }
        common = {
            "_evidence_dir": directory, "_authority": self.authority,
            "_clock": lambda: datetime(2026, 8, 4, 17, tzinfo=NY),
            "_governors": self.governors,
            "read_fn": lambda *_args: bars,
        }
        entries = (
            lambda: stock_validate.validate_series(
                "unused-bank", "TEST", "1d", rng="Max", **common),
            lambda: stock_validate.cross_validate_ticker(
                "unused-bank", "TEST", "1d", rng="Max",
                fingerprint_fn=fingerprint, **common),
            lambda: stock_validate.revalidate_library(
                "unused-bank", series=[("TEST", "1d")], rng="Max", **common),
        )
        with (tp.offline_policy() as capability,
              patch.object(stock_validate, "fetch_daily_reference",
                           side_effect=LedgerError("injected ledger failure"))):
            common["_test_capability"] = capability
            for entry in entries:
                with self.subTest(entry=entry), self.assertRaises(LedgerError):
                    entry()
        ledgers = list(directory.glob("*.jsonl"))
        self.assertEqual(len(ledgers), 3)
        self.assertTrue(all('"event":"seal"' not in path.read_text(
            encoding="utf-8") for path in ledgers))

    def test_single_request_root_reaches_guarded_attempt(self, use_default=False):
        raw = json.dumps({"data": [self.row("2026-08-03")]}).encode("utf-8")
        responses = self.a55_default_receiver(lambda url: raw) if use_default else []
        with tp.offline_policy() as capability:
            result = stock_validate.fetch_daily_reference_single_request(
                "TEST", "Max", _test_capability=capability,
                _evidence_dir=Path(self.temp.name) / "root-ledgers",
                _authority=self.authority,
                _clock=lambda: datetime(2026, 8, 4, 17, tzinfo=NY),
                _governors=self.governors, _opener=None if use_default else lambda url: raw)
        self.assertEqual(result, {"2026-08-03": (10.0, 11.0, 9.0, 10.0, 100)})
        self.assertEqual(stock_validate._REF_CACHE[("TEST", "Max")]["kind"],
                         "verified_http_reference")
        with tp.offline_policy() as capability:
            replay = stock_validate.fetch_daily_reference_single_request(
                "TEST", "Max", _test_capability=capability,
                _evidence_dir=Path(self.temp.name) / "replay-ledgers",
                _authority=self.authority,
                _clock=lambda: datetime(2026, 8, 4, 17, tzinfo=NY),
                _governors=self.governors,
                _opener=None if use_default else lambda url: self.fail("cache replay attempted transport"))
        self.assertEqual(replay, result)
        replay_ledger = next((Path(self.temp.name) / "replay-ledgers").glob(
            "*.jsonl"))
        self.assertEqual([event["event"] for event in
                          inspect_ledger(replay_ledger)["events"]], ["seal"])
        if use_default:
            self.assertEqual(len(responses), 1, "warm replay must not physically open")
            self.assertTrue(responses[0].closed)

    def test_five_year_root_reuses_only_the_sealed_result(self):
        raw = json.dumps({"data": [self.row("2026-08-03")]}).encode("utf-8")
        sent = []
        common = {
            "_authority": self.authority,
            "_clock": lambda: datetime(2026, 8, 4, 17, tzinfo=NY),
            "_governors": self.governors,
        }
        with tp.offline_policy() as capability:
            common["_test_capability"] = capability
            first = stock_validate.fetch_daily_reference_single_request(
                "TEST", "5Y", _evidence_dir=Path(self.temp.name) / "five-year",
                _opener=lambda url: sent.append(url) or raw, **common)
            replay = stock_validate.fetch_daily_reference_single_request(
                "TEST", "5Y", _evidence_dir=Path(self.temp.name) / "five-year-replay",
                _opener=lambda _url: self.fail("5Y replay attempted transport"),
                **common)
        self.assertEqual(first, replay)
        self.assertEqual(sent, [stockanalysis_url("TEST", "5Y")])
        entry = stock_validate._REF_CACHE[("TEST", "5Y")]
        self.assertEqual(entry["range"], "5Y")
        self.assertTrue(entry["attempt_id"] and entry["ledger_path"])
        replay_ledger = next((Path(self.temp.name) / "five-year-replay").glob(
            "*.jsonl"))
        self.assertEqual([event["event"] for event in
                          inspect_ledger(replay_ledger)["events"]], ["seal"])

    def test_corrupted_verified_reference_cache_is_a_miss(self):
        raw = json.dumps({"data": [self.row("2026-08-03")]}).encode("utf-8")
        seen = []
        options = {
            "_evidence_dir": Path(self.temp.name) / "cache-ledgers",
            "_authority": self.authority,
            "_clock": lambda: datetime(2026, 8, 4, 17, tzinfo=NY),
            "_governors": self.governors,
            "_opener": lambda url: seen.append(url) or raw,
        }
        with tp.offline_policy() as capability:
            options["_test_capability"] = capability
            first = stock_validate.fetch_daily_reference_single_request(
                "TEST", "Max", **options)
            hit = stock_validate._REF_CACHE[("TEST", "Max")]
            hit["rows"][0]["label"] = "not-a-date"
            hit["accepted_digest"] = digest_value(hit["rows"])
            second = stock_validate.fetch_daily_reference_single_request(
                "TEST", "Max", **options)
        self.assertEqual(first, second)
        self.assertEqual(seen, [stockanalysis_url("TEST", "Max")] * 2)

    def test_reference_cache_requires_sealed_attempt_result_provenance(self):
        raw = json.dumps({"data": [self.row("2026-08-03")]}).encode("utf-8")
        sent = []
        key = ("TEST", "Max")
        common = {
            "_authority": self.authority,
            "_clock": lambda: datetime(2026, 8, 4, 17, tzinfo=NY),
            "_governors": self.governors,
            "_opener": lambda url: sent.append(url) or raw,
        }
        with tp.offline_policy() as capability:
            common["_test_capability"] = capability
            first = stock_validate.fetch_daily_reference_single_request(
                "TEST", "Max", _evidence_dir=Path(self.temp.name) / "first",
                **common)
            good = deepcopy(stock_validate._REF_CACHE[key])
            self.assertIn("attempt_id", good)
            self.assertIn("ledger_path", good)
            for field, replacement in (("operation_id", "wrong-operation"),
                                       ("attempt_id", "wrong-attempt"),
                                       ("source_bytes_sha256", "0" * 64)):
                with self.subTest(field=field):
                    bad = deepcopy(good)
                    bad[field] = replacement
                    stock_validate._REF_CACHE[key] = bad
                    again = stock_validate.fetch_daily_reference_single_request(
                        "TEST", "Max",
                        _evidence_dir=Path(self.temp.name) / field,
                        **common)
                    self.assertEqual(again, first)
                    self.assertEqual(len(sent), 2 +
                                     ("operation_id", "attempt_id",
                                      "source_bytes_sha256").index(field))
            stock_validate._REF_CACHE[key] = deepcopy(good)
            Path(good["ledger_path"]).with_suffix(".seal.json").write_bytes(
                b"damaged seal receipt\n")
            again = stock_validate.fetch_daily_reference_single_request(
                "TEST", "Max", _evidence_dir=Path(self.temp.name) / "receipt",
                **common)
            self.assertEqual(again, first)
            self.assertEqual(len(sent), 5)

        mutant = deepcopy(stock_validate._REF_CACHE[key])
        mutant["operation_id"] = "wrong-operation"
        stock_validate._REF_CACHE[key] = mutant
        with (tp.offline_policy() as capability,
              patch.object(stock_validate, "_reference_ledger_attempt",
                           return_value=mutant["attempt_id"])):
            common["_test_capability"] = capability
            before = len(sent)
            replay = stock_validate.fetch_daily_reference_single_request(
                "TEST", "Max", _evidence_dir=Path(self.temp.name) / "mutant",
                **common)
            self.assertEqual(replay, first)
            with self.assertRaises(AssertionError):
                self.assertEqual(len(sent), before + 1)
        print("A23A_REF_PROVENANCE_INVERSE_RED skipped-ledger-pair-check")

    def test_mixed_malformed_row_never_reaches_either_consumer_cache(self):
        raw = json.dumps({"data": [self.row("2026-08-03"),
                                   self.row("nonsense")]}).encode("utf-8")
        validation_ledger = Path(self.temp.name) / "malformed-validation"
        with tp.offline_policy() as capability:
            with self.assertRaises(ResponseQuarantined):
                stock_validate.fetch_daily_reference_single_request(
                    "TEST", "Max", _test_capability=capability,
                    _evidence_dir=validation_ledger, _authority=self.authority,
                    _clock=lambda: datetime(2026, 8, 4, 17, tzinfo=NY),
                    _governors=self.governors, _opener=lambda _url: raw)
        self.assertNotIn(("TEST", "Max"), stock_validate._REF_CACHE)
        ledger = next(validation_ledger.glob("*.jsonl"))
        events = inspect_ledger(ledger, require_seal=False)["events"]
        self.assertEqual([event["event"] for event in events],
                         ["decision", "result"])
        self.assertEqual(events[-1]["payload"]["outcome"], "error")
        self.assertEqual(len(list(validation_ledger.glob("*.quarantine.json"))), 1)

        project = Path(self.temp.name) / "malformed-sweep"
        bank = project / "bank"
        cache_root = project / "cache"
        cache_root.mkdir(parents=True)
        snapshot = {
            "ticker": "TEST", "conid": 101, "provider_symbol": "TEST",
            "manifest_fingerprint": "a" * 64,
            "stored": {"2026-08-03": 10.0}, "stored_rows": 1,
            "stored_first": "2026-08-03", "stored_last": "2026-08-03",
            "stored_basis": "raw", "basis_actions_applied": [],
        }
        identity = {key: snapshot[key] for key in
                    ("ticker", "conid", "provider_symbol",
                     "manifest_fingerprint")}
        previous = external_sweep._cache_path(cache_root, identity)
        previous.write_bytes(b"last-good bytes\n")
        sweep_ledger = project / "ledgers"
        provider = external_sweep.StockAnalysisProvider(
            opener=lambda _url, _timeout: raw,
            now=lambda: datetime(2026, 8, 4, tzinfo=timezone.utc))
        with (tp.offline_policy() as capability,
              patch.object(external_sweep, "stored_daily_snapshot",
                           return_value=snapshot),
              patch.object(external_sweep, "write_cache") as write_cache):
            row = external_sweep.sweep_ticker(
                bank, "TEST", provider=provider, cache_root=cache_root,
                _test_capability=capability, _evidence_dir=sweep_ledger,
                _authority=self.authority,
                _clock=lambda: datetime(2026, 8, 4, 17, tzinfo=NY),
                _governors=self.governors)
        self.assertEqual(row["verdict"], "UNVERIFIABLE")
        self.assertEqual(row["error_type"], "ResponseQuarantined")
        self.assertFalse(Path(row["quarantine"]).is_absolute())
        self.assertTrue((sweep_ledger / row["quarantine"]).is_file())
        write_cache.assert_not_called()
        self.assertEqual(previous.read_bytes(), b"last-good bytes\n")
        ledger = next(sweep_ledger.glob("*.jsonl"))
        events = inspect_ledger(ledger)["events"]
        self.assertEqual([event["event"] for event in events],
                         ["decision", "result", "seal"])
        self.assertEqual(events[1]["payload"]["outcome"], "error")
        self.assertEqual(len(list(sweep_ledger.glob("*.quarantine.json"))), 1)

    def test_revalidate_library_quarantines_one_ticker_and_seals_clean_second(self):
        bad = json.dumps({"data": [self.row("2026-08-03"),
                                   self.row("nonsense")]}).encode("utf-8")
        good = json.dumps({"data": [self.row("2026-08-03")]}).encode("utf-8")
        bars = [(datetime(2026, 8, 3), 10.0, 11.0, 9.0, 10.0, 100)]
        sent = []

        def opener(url):
            ticker = "BAD" if "/s/BAD/" in url else "GOOD"
            sent.append(ticker)
            return bad if ticker == "BAD" else good

        def run(directory):
            with tp.offline_policy() as capability:
                return stock_validate.revalidate_library(
                    "unused-bank", series=[("BAD", "1d"), ("GOOD", "1d")],
                    rng="Max", read_fn=lambda *_args: bars,
                    _test_capability=capability, _evidence_dir=directory,
                    _authority=self.authority,
                    _clock=lambda: datetime(2026, 8, 4, 17, tzinfo=NY),
                    _governors=self.governors, _opener=opener)

        directory = Path(self.temp.name) / "batch-validation"
        report = run(directory)
        rows = {row["ticker"]: row for row in report["results"]}
        self.assertEqual(sent, ["BAD", "GOOD"])
        self.assertEqual(report["summary"]["total"], 2)
        self.assertEqual(rows["BAD"]["status"], "error")
        self.assertEqual(rows["BAD"]["error_type"], "ResponseQuarantined")
        self.assertFalse(Path(rows["BAD"]["quarantine"]).is_absolute())
        self.assertTrue((directory / rows["BAD"]["quarantine"]).is_file())
        self.assertEqual(rows["BAD"]["checked"], 0)
        self.assertEqual(rows["GOOD"]["status"], "ok")
        self.assertEqual(rows["GOOD"]["checked"], 1)
        self.assertNotIn(("BAD", "Max"), stock_validate._REF_CACHE)
        self.assertIn(("GOOD", "Max"), stock_validate._REF_CACHE)
        audit = inspect_ledger(next(directory.glob("*.jsonl")))
        self.assertEqual([event["event"] for event in audit["events"]],
                         ["decision", "result", "decision", "result", "seal"])
        self.assertEqual([audit["events"][i]["payload"]["outcome"]
                          for i in (1, 3)], ["error", "returned"])
        self.assertEqual(len(list(directory.glob("*.quarantine.json"))), 1)

        normalizer = labels.normalize_stockanalysis_rows
        def old_abort(value):
            try:
                return normalizer(value)
            except ResponseQuarantined as exc:
                raise AuthorityError(str(exc)) from exc
        sent.clear()
        with patch.object(labels, "normalize_stockanalysis_rows",
                          side_effect=old_abort):
            with self.assertRaises(AuthorityError):
                run(Path(self.temp.name) / "old-abort-validation")
        self.assertEqual(sent, ["BAD"])
        print("A23A_BATCH_INVERSE_RED validation-whole-root-abort")

        sent.clear()
        with patch.object(FetchLedger, "quarantine", return_value=None):
            with self.assertRaisesRegex(LedgerError, "quarantine failed"):
                run(Path(self.temp.name) / "quarantine-fail-validation")
        self.assertEqual(sent, ["BAD"])
        self.assertNotIn(("BAD", "Max"), stock_validate._REF_CACHE)

        for shape, bad in self.malformed_shapes().items():
            with self.subTest(shape=shape):
                stock_validate.clear_reference_cache()
                sent.clear()
                shape_dir = Path(self.temp.name) / ("validation-" + shape)
                shape_report = run(shape_dir)
                shape_rows = {row["ticker"]: row for row in
                              shape_report["results"]}
                self.assertEqual(sent, ["BAD", "GOOD"])
                self.assertEqual(shape_rows["BAD"]["error_type"],
                                 "ResponseQuarantined")
                locator = shape_rows["BAD"]["quarantine"]
                self.assertFalse(Path(locator).is_absolute())
                self.assertTrue((shape_dir / locator).is_file())
                self.assertEqual(shape_rows["GOOD"]["status"], "ok")
                self.assertNotIn(("BAD", "Max"), stock_validate._REF_CACHE)
                self.assertIn(("GOOD", "Max"), stock_validate._REF_CACHE)
                self.assertEqual(len(list(shape_dir.glob(
                    "*.quarantine.json"))), 1)
                self.assertEqual(len(inspect_ledger(next(shape_dir.glob(
                    "*.jsonl")))["events"]), 5)

        bad = self.malformed_shapes()["infinite-date"]
        sent.clear()
        with patch.object(labels, "ResponseQuarantined", ValueError):
            old_shape_report = run(Path(self.temp.name) /
                                   "validation-untyped-inverse")
        old_shape_rows = {row["ticker"]: row for row in
                          old_shape_report["results"]}
        with self.assertRaises(AssertionError):
            self.assertEqual(old_shape_rows["BAD"].get("error_type"),
                             "ResponseQuarantined")
        print("A23A_SHAPE_INVERSE_RED validation-untyped-response")

    def test_revalidate_library_mid_batch_terminal_fault_keeps_cache_unpublished(self):
        raw = json.dumps({"data": [self.row("2026-08-03")]}).encode("utf-8")
        bars = [(datetime(2026, 8, 3), 10.0, 11.0, 9.0, 10.0, 100)]
        for error_type in (AuthorityError, RequestCancelled):
            with self.subTest(error=error_type.__name__):
                stock_validate.clear_reference_cache()
                directory = Path(self.temp.name) / (
                    "validation-mid-batch-" + error_type.__name__)
                sent = []

                def opener(url):
                    ticker = next(name for name in ("AAA", "BBB", "CCC")
                                  if f"/s/{name}/" in url)
                    sent.append(ticker)
                    if ticker == "BBB":
                        raise error_type("injected mid-batch terminal fault")
                    return raw

                with tp.offline_policy() as capability:
                    with self.assertRaisesRegex(
                            error_type, "injected mid-batch terminal fault"):
                        stock_validate.revalidate_library(
                            "unused-bank", series=[("AAA", "1d"),
                                                   ("BBB", "1d"),
                                                   ("CCC", "1d")],
                            rng="Max", read_fn=lambda *_args: bars,
                            _test_capability=capability,
                            _evidence_dir=directory,
                            _authority=self.authority,
                            _clock=lambda: datetime(2026, 8, 4, 17,
                                                    tzinfo=NY),
                            _governors=self.governors, _opener=opener)
                self.assertEqual(sent, ["AAA", "BBB"])
                self.assertNotIn(("AAA", "Max"), stock_validate._REF_CACHE)
                audit = inspect_ledger(next(directory.glob("*.jsonl")),
                                       require_seal=False)
                self.assertFalse(audit["verified"])
                self.assertEqual([event["event"] for event in audit["events"]],
                                 ["decision", "result", "decision", "result"])
                self.assertEqual(audit["events"][-1]["payload"]["error_type"],
                                 error_type.__name__)

    def test_sweep_bank_mid_batch_terminal_fault_keeps_cache_and_report_unpublished(self):
        project = Path(self.temp.name) / "sweep-mid-batch"
        bank = project / "bank"
        logs = project / "Run Logs"
        cache = logs / "cache"
        cache.mkdir(parents=True)
        days = []
        for month_index in range(12):
            year = 2025 + (8 + month_index - 1) // 12
            month = (8 + month_index - 1) % 12 + 1
            day = date(year, month, 3)
            while self.authority.window("1d", day) is None:
                day += timedelta(days=1)
            days.append(day.isoformat())
        raw = json.dumps({"data": [self.row(day, 100) for day in days]}).encode(
            "utf-8")
        snapshots = {
            name: {"ticker": name, "conid": 100 + index,
                   "provider_symbol": name,
                   "manifest_fingerprint": str(index) * 64,
                   "stored": {day: 100.0 for day in days},
                   "stored_rows": len(days), "stored_first": min(days),
                   "stored_last": max(days), "stored_basis": "raw",
                   "basis_actions_applied": []}
            for index, name in enumerate(("AAA", "BBB", "CCC"), 1)
        }
        for error_type in (AuthorityError, RequestCancelled):
            with self.subTest(error=error_type.__name__):
                directory = logs / error_type.__name__
                sent = []

                def opener(url, _timeout):
                    ticker = next(name for name in ("AAA", "BBB", "CCC")
                                  if f"/s/{name}/" in url)
                    sent.append(ticker)
                    if ticker == "BBB":
                        raise error_type("injected mid-batch terminal fault")
                    return raw

                provider = external_sweep.StockAnalysisProvider(
                    opener=opener,
                    now=lambda: datetime(2026, 8, 4, tzinfo=timezone.utc))
                with (tp.offline_policy() as capability,
                      patch.object(external_sweep, "stored_daily_snapshot",
                                   side_effect=lambda _root, ticker:
                                   snapshots[ticker]),
                      patch.object(external_sweep, "_manifest_unchanged",
                                   return_value=True),
                      patch.object(external_sweep, "_manifest_record",
                                   side_effect=lambda _root, ticker:
                                   snapshots[ticker]),
                      patch.object(external_sweep, "write_cache") as cache_write,
                      patch.object(external_sweep, "write_artifact") as report_write):
                    with self.assertRaisesRegex(
                            error_type, "injected mid-batch terminal fault"):
                        external_sweep.sweep_bank(
                            bank, ["AAA", "BBB", "CCC"], provider=provider,
                            cache_root=cache, run_logs_root=logs,
                            gate_path=logs / ("." + error_type.__name__ + ".lock"),
                            now=datetime(2026, 8, 4, tzinfo=timezone.utc),
                            clock=self.clock.now, sleep_fn=self.clock.sleep,
                            _test_capability=capability,
                            _evidence_dir=directory,
                            _authority=self.authority,
                            _clock=lambda: datetime(2026, 8, 4, 17,
                                                    tzinfo=NY),
                            _governors=self.governors)
                self.assertEqual(sent, ["AAA", "BBB"])
                cache_write.assert_not_called()
                report_write.assert_not_called()
                audit = inspect_ledger(next(directory.glob("*.jsonl")),
                                       require_seal=False)
                self.assertFalse(audit["verified"])
                self.assertEqual([event["event"] for event in audit["events"]],
                                 ["decision", "result", "decision", "result"])
                self.assertEqual(audit["events"][-1]["payload"]["error_type"],
                                 error_type.__name__)

    def test_direct_cross_quarantine_keeps_prior_verdict_and_debt(self):
        import addstock_run_manifest as addstock
        import fetch_authority as authority_module

        bank = Path(self.temp.name) / "direct-cross-bank"
        bank.mkdir()
        prior = {"ticker": "TEST", "interval": "1d", "status": "discrepancy",
                 "interval_fingerprint": {"current": True,
                                          "after_sha256": "b" * 64}}
        sidecar = stock_validate.record_cross_validation(bank, prior)
        self.assertIsNotNone(sidecar)
        original_bytes = Path(sidecar).read_bytes()

        def fingerprint(_root, ticker, interval):
            return {"schema_version": stock_validate.ss.INTERVAL_FINGERPRINT_VERSION,
                    "algorithm": "sha256", "sha256": "a" * 64,
                    "ticker": ticker, "interval": interval, "present": True,
                    "backfill_incomplete": False, "month_count": 1,
                    "verified_absent_count": 0}

        def debt():
            return addstock.current_xval_intervals(
                bank, stock_validate.load_cross_validation(bank).values(),
                fingerprint_fn=fingerprint)

        self.assertEqual(debt(), [])
        raw = json.dumps({"data": [self.row("nonsense")]}).encode("utf-8")
        sent = []

        def run(directory, capability):
            return stock_validate.cross_validate_ticker(
                bank, "TEST", "1d", rng="Max",
                fingerprint_fn=fingerprint,
                _test_capability=capability, _evidence_dir=directory,
                _authority=self.authority,
                _clock=lambda: datetime(2026, 8, 4, 17, tzinfo=NY),
                _governors=self.governors,
                _opener=lambda url: sent.append(url) or raw)

        directory = Path(self.temp.name) / "direct-cross-ledgers"
        with tp.offline_policy() as capability:
            with self.assertRaises(ResponseQuarantined) as caught:
                run(directory, capability)
        self.assertEqual(len(sent), 1)
        self.assertFalse(Path(caught.exception.quarantine_path).is_absolute())
        self.assertTrue((directory / caught.exception.quarantine_path).is_file())
        self.assertEqual(Path(sidecar).read_bytes(), original_bytes)
        self.assertEqual(debt(), [])
        audit = inspect_ledger(next(directory.glob("*.jsonl")), require_seal=False)
        self.assertEqual([event["event"] for event in audit["events"]],
                         ["decision", "result"])

        with (tp.offline_policy() as capability,
              patch.object(authority_module, "ResponseQuarantined",
                           AuthorityError)):
            accidental = run(Path(self.temp.name) / "cross-unavailable-inverse",
                             capability)
        self.assertEqual(accidental["status"], "unavailable")
        stock_validate.record_cross_validation(bank, accidental)
        self.assertNotEqual(Path(sidecar).read_bytes(), original_bytes)
        self.assertEqual(debt(), ["1d"])
        print("A23A_DIRECT_QUARANTINE_INVERSE_RED unavailable-credits-debt")

    def test_direct_quarantine_gui_text_and_debt_stop_source_choice(self):
        source = (Path(__file__).resolve().parents[1] /
                  "display_data.py").read_text(encoding="utf-8")
        module = ast.parse(source)
        viewer = next(node for node in module.body
                      if isinstance(node, ast.ClassDef)
                      and node.name == "DataViewerApp")
        methods = {node.name: ast.get_source_segment(source, node)
                   for node in viewer.body
                   if isinstance(node, (ast.FunctionDef,
                                        ast.AsyncFunctionDef))}

        def verify(parts):
            revalidate = parts["_storage_xval_revalidate"]
            assert revalidate.index("cross_validate_ticker(") < revalidate.index(
                "record_cross_validation(")
            assert 'f"Re-validation failed:\\n{payload}"' in revalidate
            worker = parts["_xval_worker"]
            assert worker.index("cross_validate_ticker(") < worker.index(
                "pending.append(entry)")
            assert 'f"cross-check failed for {ticker} {interval}: {exc}"' in worker
            debt = parts["_addstock_consume_portfree_debt"]
            assert debt.index("cross_validate_ticker(") < debt.index(
                "record_cross_validation_many(")
            assert re.search(
                r'"notes": \[f"verification debt failed: \{exc\}"\],\s*'
                r'"cancelled": False,\s*"completed": False', debt)
            assert "block_new_build = not (completed and active is None)" in debt
            probe = parts["_storage_find_start_fill"]
            assert probe.index("cross_validate_ticker(") < probe.index(
                "record_cross_validation_many(")
            assert probe.index("record_cross_validation_many(") < probe.index(
                "_addstock_credit(")

        verify(methods)
        for label, owner, old, new in (
                ("unavailable-popup", "_storage_xval_revalidate",
                 'f"Re-validation failed:\\n{payload}"',
                 '"not on the online source"'),
                ("lost-debt-stop", "_addstock_consume_portfree_debt",
                 '"notes": [f"verification debt failed: {exc}"],\n'
                 '                    "cancelled": False,\n'
                 '                    "completed": False',
                 '"notes": [f"verification debt failed: {exc}"],\n'
                 '                    "cancelled": False,\n'
                 '                    "completed": True'),
                ("lost-worker-message", "_xval_worker",
                 'f"cross-check failed for {ticker} {interval}: {exc}"', '"unavailable"'),
                ("lost-build-block", "_addstock_consume_portfree_debt",
                 "block_new_build = not (completed and active is None)",
                 "block_new_build = False"),
                ("credit-before-record", "_storage_find_start_fill",
                 "record_cross_validation_many(",
                 "_addstock_credit(); record_cross_validation_many(")):
            changed = dict(methods)
            self.assertEqual(changed[owner].count(old), 1)
            changed[owner] = changed[owner].replace(old, new, 1)
            with self.subTest(mutant=label), self.assertRaises(AssertionError):
                verify(changed)
            print("A23A_GUI_TEXT_INVERSE_RED", label)

    def test_quarantine_locator_survives_project_relocation(self):
        import fetch_run_context as run_context

        raw = json.dumps({"data": [self.row("nonsense")]}).encode("utf-8")
        project = Path(self.temp.name) / "locator-project"
        logs = project / "Run Logs"
        logs.mkdir(parents=True)
        project_patch = patch.object(run_context, "PROJECT_ROOT", project)
        project_patch.start()
        self.addCleanup(project_patch.stop)
        with tempfile.TemporaryDirectory(
                dir=logs, prefix="row79-locator-") as folder:
            evidence = Path(folder) / "ledgers"
            with tp.offline_policy() as capability:
                with self.assertRaises(ResponseQuarantined) as caught:
                    stock_validate.fetch_daily_reference_single_request(
                        "TEST", "Max", _test_capability=capability,
                        _evidence_dir=evidence, _authority=self.authority,
                        _clock=lambda: datetime(2026, 8, 4, 17, tzinfo=NY),
                        _governors=self.governors, _opener=lambda _url: raw)
            locator = caught.exception.quarantine_path
            self.assertFalse(Path(locator).is_absolute())
            self.assertTrue(locator.startswith("Run Logs/"))
            source = run_context.PROJECT_ROOT / locator
            self.assertTrue(source.is_file())
            with tempfile.TemporaryDirectory() as moved:
                relocated = Path(moved) / locator
                relocated.parent.mkdir(parents=True)
                shutil.copy2(source, relocated)
                self.assertEqual(relocated.read_bytes(), source.read_bytes())

    def test_quarantine_locator_resolution_fault_is_ledger_error(self):
        import fetch_run_context as run_context

        with patch.object(Path, "resolve",
                          side_effect=OSError("injected locator fault")):
            with self.assertRaisesRegex(
                    LedgerError,
                    "response quarantine locator is unavailable") as caught:
                run_context._portable_quarantine_locator(
                    Path(self.temp.name) / "evidence.quarantine.json")
        self.assertIsInstance(caught.exception.__cause__, OSError)

    def test_sweep_bank_quarantines_one_ticker_and_seals_clean_second(self):
        import fetch_run_context as run_context

        project = Path(self.temp.name) / "batch-sweep"
        bank = project / "bank"
        (project / "Run Logs").mkdir(parents=True)
        project_patch = patch.object(run_context, "PROJECT_ROOT", project)
        project_patch.start()
        self.addCleanup(project_patch.stop)
        logs_temp = tempfile.TemporaryDirectory(
            dir=project / "Run Logs",
            prefix="row79-sweep-report-")
        self.addCleanup(logs_temp.cleanup)
        logs = Path(logs_temp.name)
        cache = logs / "cache"
        cache.mkdir(parents=True)
        days = []
        for month_index in range(12):
            year = 2025 + (8 + month_index - 1) // 12
            month = (8 + month_index - 1) % 12 + 1
            day = date(year, month, 3)
            while self.authority.window("1d", day) is None:
                day += timedelta(days=1)
            days.append(day.isoformat())
        good = json.dumps({"data": [self.row(day, 100)
                                    for day in days]}).encode("utf-8")
        bad = json.dumps({"data": [self.row(days[0], 100),
                                   self.row("nonsense")]}).encode("utf-8")
        snapshots = {
            ticker: {"ticker": ticker, "conid": conid,
                     "provider_symbol": ticker,
                     "manifest_fingerprint": char * 64,
                     "stored": {day: 100.0 for day in days},
                     "stored_rows": len(days), "stored_first": min(days),
                     "stored_last": max(days), "stored_basis": "raw",
                     "basis_actions_applied": []}
            for ticker, conid, char in (("BAD", 101, "a"),
                                        ("GOOD", 102, "b"))
        }
        previous = external_sweep._cache_path(cache, snapshots["BAD"])
        previous.write_bytes(b"last-good bytes\n")
        sent, published = [], []

        def opener(url, _timeout):
            ticker = "BAD" if "/s/BAD/" in url else "GOOD"
            sent.append(ticker)
            return bad if ticker == "BAD" else good

        provider = external_sweep.StockAnalysisProvider(
            opener=opener, now=lambda: datetime(2026, 8, 4,
                                                tzinfo=timezone.utc))
        def checked_cache(target, root, identity, payload):
            self.assertTrue(inspect_ledger(next(directory.glob(
                "*.jsonl")))["verified"])
            published.append(identity["ticker"])
            return target / (identity["ticker"] + ".json"), payload

        def run(directory):
            with (tp.offline_policy() as capability,
                  patch.object(external_sweep, "stored_daily_snapshot",
                               side_effect=lambda _root, ticker: snapshots[ticker]),
                  patch.object(external_sweep, "_manifest_unchanged",
                               return_value=True),
                  patch.object(external_sweep, "_manifest_record",
                               side_effect=lambda _root, ticker: snapshots[ticker]),
                  patch.object(external_sweep, "write_cache",
                               side_effect=checked_cache)):
                return external_sweep.sweep_bank(
                    bank, ["BAD", "GOOD"], provider=provider,
                    cache_root=cache, run_logs_root=logs,
                    owner=directory.name,
                    gate_path=logs / ".batch-sweep.lock",
                    now=datetime(2026, 8, 4, tzinfo=timezone.utc),
                    clock=self.clock.now, sleep_fn=self.clock.sleep,
                    _test_capability=capability, _evidence_dir=directory,
                    _authority=self.authority,
                    _clock=lambda: datetime(2026, 8, 4, 17, tzinfo=NY),
                    _governors=self.governors)

        directory = logs / "ledgers"
        report = run(directory)
        rows = {row["ticker"]: row for row in report["rows"]}
        self.assertEqual(sent, ["BAD", "GOOD"])
        self.assertEqual(report["network_requests"], 2)
        self.assertEqual(rows["BAD"]["verdict"], "UNVERIFIABLE")
        self.assertEqual(rows["BAD"]["error_type"], "ResponseQuarantined")
        self.assertFalse(Path(rows["BAD"]["quarantine"]).is_absolute())
        self.assertTrue((run_context.PROJECT_ROOT /
                         rows["BAD"]["quarantine"]).is_file())
        self.assertNotIn("reference_digest", rows["BAD"])
        self.assertNotEqual(rows["GOOD"]["verdict"], "UNVERIFIABLE")
        self.assertEqual(published, ["GOOD"])
        self.assertEqual(previous.read_bytes(), b"last-good bytes\n")
        audit = inspect_ledger(next(directory.glob("*.jsonl")))
        self.assertEqual([event["event"] for event in audit["events"]],
                         ["decision", "result", "decision", "result", "seal"])
        self.assertEqual(len(list(directory.glob("*.quarantine.json"))), 1)
        persisted = json.loads(Path(report["artifact"]).read_text(
            encoding="utf-8"))
        persisted_bad = next(row for row in persisted["rows"]
                             if row["ticker"] == "BAD")
        self.assertEqual(persisted_bad["quarantine"],
                         rows["BAD"]["quarantine"])
        self.assertFalse(Path(persisted_bad["quarantine"]).is_absolute())
        with tempfile.TemporaryDirectory() as moved:
            relocated = Path(moved)
            source_quarantine = (run_context.PROJECT_ROOT /
                                 persisted_bad["quarantine"])
            target_quarantine = relocated / persisted_bad["quarantine"]
            target_quarantine.parent.mkdir(parents=True)
            shutil.copy2(source_quarantine, target_quarantine)
            artifact_relative = Path(report["artifact"]).relative_to(
                run_context.PROJECT_ROOT)
            target_artifact = relocated / artifact_relative
            target_artifact.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(report["artifact"], target_artifact)
            moved_bad = next(row for row in json.loads(
                target_artifact.read_text(encoding="utf-8"))["rows"]
                if row["ticker"] == "BAD")
            self.assertEqual(moved_bad["quarantine"],
                             persisted_bad["quarantine"])
            self.assertEqual((relocated / moved_bad["quarantine"]).read_bytes(),
                             source_quarantine.read_bytes())

        normalizer = labels.normalize_stockanalysis_rows
        def old_abort(value):
            try:
                return normalizer(value)
            except ResponseQuarantined as exc:
                raise AuthorityError(str(exc)) from exc
        sent.clear()
        with patch.object(labels, "normalize_stockanalysis_rows",
                          side_effect=old_abort):
            with self.assertRaises(AuthorityError):
                run(logs / "old-abort-ledgers")
        self.assertEqual(sent, ["BAD"])
        print("A23A_BATCH_INVERSE_RED sweep-whole-root-abort")

        sent.clear()
        published.clear()
        with patch.object(FetchLedger, "quarantine", return_value=None):
            with self.assertRaisesRegex(LedgerError, "quarantine failed"):
                run(logs / "quarantine-fail-ledgers")
        self.assertEqual(sent, ["BAD"])
        self.assertEqual(published, [])

        for shape, bad in self.malformed_shapes().items():
            with self.subTest(shape=shape):
                sent.clear()
                published.clear()
                shape_dir = logs / ("sweep-" + shape)
                directory = shape_dir
                shape_report = run(shape_dir)
                shape_rows = {row["ticker"]: row for row in
                              shape_report["rows"]}
                self.assertEqual(sent, ["BAD", "GOOD"])
                self.assertEqual(shape_report["network_requests"], 2)
                self.assertEqual(shape_rows["BAD"]["error_type"],
                                 "ResponseQuarantined")
                locator = shape_rows["BAD"]["quarantine"]
                self.assertFalse(Path(locator).is_absolute())
                self.assertTrue((run_context.PROJECT_ROOT / locator).is_file())
                self.assertEqual(published, ["GOOD"])
                self.assertEqual(previous.read_bytes(), b"last-good bytes\n")
                self.assertEqual(len(list(shape_dir.glob(
                    "*.quarantine.json"))), 1)
                self.assertEqual(len(inspect_ledger(next(shape_dir.glob(
                    "*.jsonl")))["events"]), 5)

        bad = self.malformed_shapes()["infinite-date"]
        sent.clear()
        published.clear()
        directory = logs / "sweep-untyped-inverse"
        with patch.object(labels, "ResponseQuarantined", ValueError):
            old_shape_report = run(directory)
        old_shape_rows = {row["ticker"]: row for row in
                          old_shape_report["rows"]}
        with self.assertRaises(AssertionError):
            self.assertEqual(sent, ["BAD", "GOOD"])
        self.assertNotEqual(old_shape_rows["BAD"].get("error_type"),
                            "ResponseQuarantined")
        print("A23A_SHAPE_INVERSE_RED sweep-untyped-retry")

    def a45_sweep_retry_witness(self, *, failures=2, attempts=3, terminal=None):
        """Real provider/guard/ledger, with only its physical sender replaced."""
        raw = json.dumps({"data": [self.row("2026-08-03")]}).encode()
        sent, waits = [], []
        pacer = Mock()
        def opener(url, timeout):
            sent.append((url, timeout))
            if terminal is not None:
                raise terminal("A45 terminal transport witness")
            if len(sent) <= failures:
                raise TimeoutError("A45 retryable transport witness")
            return raw
        provider = external_sweep.StockAnalysisProvider(opener=opener,
            now=lambda: datetime(2026, 8, 4, tzinfo=timezone.utc))
        def sleep(seconds):
            waits.append(seconds)
            self.clock.sleep(seconds)
        with tp.offline_policy() as capability:
            context = self.context(capability=capability)
            worker = context.worker("a45-sweep")
            if terminal is not None:
                with self.assertRaisesRegex(terminal, "A45 terminal transport witness"):
                    external_sweep._fetch_with_retry(provider, "TEST", pacer,
                        attempts, 1, sleep_fn=sleep, _fetch_worker=worker)
                expected, outcomes = 1, ["error"]
            elif failures >= attempts:
                with self.assertRaises(external_sweep.ProviderError) as caught:
                    external_sweep._fetch_with_retry(provider, "TEST", pacer,
                        attempts, 1, sleep_fn=sleep, _fetch_worker=worker)
                self.assertEqual(caught.exception.attempts, attempts)
                expected, outcomes = attempts, ["timeout"] * attempts
            else:
                result, count = external_sweep._fetch_with_retry(provider, "TEST", pacer,
                    attempts, 1, sleep_fn=sleep, _fetch_worker=worker)
                self.assertEqual(count, failures + 1)
                self.assertEqual(result["reference"], {"2026-08-03": 10.0})
                expected = failures + 1
                outcomes = ["timeout"] * failures + ["returned"]
            context.seal()
        audit = inspect_ledger(context.ledger.path)
        events = audit["events"][:-1]
        self.assertEqual([row["event"] for row in events], ["decision", "result"] * expected)
        self.assertEqual(sent, [(stockanalysis_url("TEST", "Max"), 20.0)] * expected)
        self.assertEqual(pacer.wait.call_count, expected)
        self.assertEqual(waits, [2 ** index for index in range(expected - 1)])
        self.assertEqual([row["payload"]["outcome"] for row in events[1::2]], outcomes)
        self.assertEqual({row["producer_id"] for row in events}, {"http.stockanalysis.external_sweep"})
        self.assertEqual(len({row["attempt_id"] for row in events}), expected)
        self.assertEqual(len({row["logical_id"] for row in events}), 1,
                         "same sweep request must retain one logical ID across retries")
        for decision, result in zip(events[::2], events[1::2]):
            self.assertEqual(decision["attempt_id"], result["attempt_id"])
        self.assertEqual(stock_validate._REF_CACHE, {})
        print("A45_SWEEP_BUDGET", expected, outcomes)

    def test_a45_sweep_retry_success_has_one_logical_request(self):
        self.a45_sweep_retry_witness()

    def test_a45_sweep_retry_exhaustion_has_exact_five_attempts(self):
        self.a45_sweep_retry_witness(failures=5, attempts=5)

    def test_a45_sweep_authority_error_is_not_retried(self):
        self.a45_sweep_retry_witness(terminal=AuthorityError)

    def test_a45_sweep_ledger_error_is_not_retried(self):
        self.a45_sweep_retry_witness(terminal=LedgerError)

    def test_a45_sweep_cancellation_is_not_retried(self):
        self.a45_sweep_retry_witness(terminal=RequestCancelled)

    def test_a45_sweep_refusal_is_not_retried(self):
        self.a45_sweep_retry_witness(terminal=RequestRefused)

    def test_a45_retry_binding_rejects_type_worker_producer_symbol_and_range(self):
        from dataclasses import replace
        with tp.offline_policy() as capability:
            context = self.context(capability=capability)
            worker = context.worker("a45-owner")
            other = context.worker("a45-other")
            producer = "http.stockanalysis.external_sweep"
            retained = labels.stockanalysis_request(worker, producer, "TEST", "Max")
            sender = Mock(return_value=b'{}')
            cases = (
                (object(), worker, producer, "TEST", "Max"),
                (retained, other, producer, "TEST", "Max"),
                (retained, worker, "http.stockanalysis.validation", "TEST", "Max"),
                (retained, worker, producer, "OTHER", "Max"),
                (retained, worker, producer, "TEST", "5Y"),
                (replace(retained, request=object()), worker, producer, "TEST", "Max"),
                (replace(retained, url=retained.url+'bad'), worker, producer, "TEST", "Max"),
                (replace(retained, range="5Y"), worker, producer, "TEST", "Max"),
            )
            for value, owner, kind, symbol, rng in cases:
                with self.subTest(kind=kind, symbol=symbol, rng=rng), self.assertRaisesRegex(
                        RequestRefused, "retry request binding differs"):
                    labels.guarded_stockanalysis_attempt(owner, kind, symbol, rng,
                        sender, _logical_request=value)
            sender.assert_not_called()
            self.assertEqual(context.ledger.path.read_bytes(), b'')
            context.seal()

    def test_a45_retry_rechecks_admission_and_offline_gate(self):
        with tp.offline_policy() as capability:
            context = self.context(capability=capability)
            worker = context.worker("a45-owner")
            producer = "http.stockanalysis.external_sweep"
            retained = labels.stockanalysis_request(worker, producer, "TEST", "Max")
            sender = Mock(return_value=b'{}')
            with patch.object(FetchRunContext, "permits", return_value=False), self.assertRaisesRegex(
                    RequestRefused, "retry request admission refused"):
                labels.guarded_stockanalysis_attempt(worker, producer, "TEST", "Max",
                    sender, _logical_request=retained)
        with self.assertRaisesRegex(AuthorityError, "offline policy capability expired"):
            labels.guarded_stockanalysis_attempt(worker, producer, "TEST", "Max",
                sender, _logical_request=retained)
        sender.assert_not_called()
        self.assertEqual(context.ledger.path.read_bytes(), b'')

    def test_a45_fresh_request_per_retry_inverse_is_red(self):
        mutant = _source_inverse(external_sweep._fetch_with_retry,
            "_logical_request=logical_request", "_logical_request=None")
        with patch.object(external_sweep, "_fetch_with_retry", mutant), self.assertRaisesRegex(
                AssertionError, "same sweep request must retain one logical ID"):
            self.a45_sweep_retry_witness()
        print("A45_INVERSE_RED fresh-request-per-retry")

    def a46_status_witness(self, *, status=429, retry_after='10', failures=2,
                           terminal=None):
        raw = json.dumps({'data': [self.row('2026-08-03')]}).encode()
        sent, faults, policy_calls = [], [], []
        base = datetime(2026, 8, 4, 21, tzinfo=timezone.utc)
        headers = Message()
        if retry_after is not None:
            headers['Retry-After'] = retry_after

        def opener(url, timeout):
            sent.append((self.clock.now(), url, timeout))
            if len(sent) <= failures:
                fault = HTTPError(url, status, 'status witness', headers, None)
                self.addCleanup(fault.close)
                self.addCleanup(fault.close)
                faults.append(fault)
                raise fault
            return raw

        with tp.offline_policy() as capability:
            context = self.context(capability=capability)
            worker = context.worker('a46-status')
            governor = self.governors.provider('stockanalysis.history')
            original = governor.http_backoff

            def backoff(code, **kwargs):
                policy_calls.append((code, dict(kwargs)))
                audit = inspect_ledger(context.ledger.path, require_seal=False)
                self.assertEqual([row['event'] for row in audit['events']],
                                 ['decision', 'result'] * len(sent),
                                 'HTTP outcome must be durable before policy')
                return original(code, now=base+timedelta(seconds=self.clock.now()), **kwargs)

            provider = external_sweep.StockAnalysisProvider(opener=opener, now=lambda: base)
            with (patch.object(governor, 'http_backoff', side_effect=backoff),
                  patch.object(governor, 'saturate', wraps=governor.saturate) as saturated):
                caught = None
                try:
                    value, count = external_sweep._fetch_with_retry(provider, 'TEST', Mock(),
                        5, 1, sleep_fn=self.clock.sleep, _fetch_worker=worker)
                except Exception as exc:
                    caught = exc
                if terminal:
                    self.assertIsInstance(caught, AuthorityError,
                                          'server policy must terminate before another send')
                    self.assertIn(terminal, str(caught))
                    expected = 1
                elif failures == 5:
                    self.assertIsInstance(caught, external_sweep.ProviderError)
                    self.assertIs(caught.__cause__, faults[-1])
                    self.assertEqual(caught.attempts, 5)
                    expected = 5
                else:
                    self.assertIsNone(caught)
                    self.assertEqual((value['reference'], count), ({'2026-08-03': 10.0}, failures+1))
                    expected = failures+1
                self.assertEqual(len(sent), expected)
                self.assertEqual([row[0] for row in policy_calls], [status]*min(failures, expected),
                                 'every HTTP status must reach the canonical governor')
                self.assertEqual([row[1]['attempt'] for row in policy_calls],
                                 list(range(1, min(failures, expected)+1)),
                                 'HTTP retry ordinal must follow the retained request')
                self.assertTrue(all(row[1]['max_wait_s'] == 60.0 for row in policy_calls))
                self.assertEqual(saturated.call_count,
                    min(failures, expected) if status == 429 or
                    (status == 503 and retry_after is not None) else 0,
                    'malformed retry value must preserve saturation')
            events = inspect_ledger(context.ledger.path, require_seal=False)['events']
            self.assertEqual([row['event'] for row in events], ['decision', 'result']*expected)
            self.assertEqual(len({row['logical_id'] for row in events}), 1)
            self.assertEqual(len({row['attempt_id'] for row in events}), expected)
            for index, event in enumerate(events[1::2][:len(faults)], 1):
                self.assertEqual(event['payload']['error_type'], 'HTTPError')
                self.assertEqual(event['payload'].get('http_error'),
                    {'status': status, 'retry_after': retry_after, 'attempt': index},
                    'actual HTTP status evidence must survive policy handling')
            self.assertTrue(all(row[1:] == (stockanalysis_url('TEST', 'Max'), 20.0) for row in sent))
            self.assertEqual(stock_validate._REF_CACHE, {})
            print('A46_HTTP_STATUS', status, repr(retry_after), 'sends', expected,
                  'times', [row[0] for row in sent])
            return sent, governor

    def test_a46_numeric_retry_after_delays_real_sweep_attempts(self):
        sent, _ = self.a46_status_witness()
        self.assertEqual([row[0] for row in sent], [0.0, 10.0, 20.0])

    def test_a46_http_date_retry_after_delays_real_sweep_attempts(self):
        sent, _ = self.a46_status_witness(status=503,
            retry_after='Tue, 04 Aug 2026 21:00:10 GMT', failures=1)
        self.assertEqual([row[0] for row in sent], [0.0, 10.0])

    def test_a46_long_retry_after_stops_and_preserves_shared_deadline(self):
        sent, governor = self.a46_status_witness(retry_after='120', terminal='wait budget')
        self.assertEqual(len(sent), 1)
        self.assertEqual(governor._pacer._not_before, 120.0)
        self.assertIs(governor, self.governors.provider('stockanalysis.history'))

    def test_a46_malformed_retry_after_stops_without_second_send(self):
        for status in (429, 503):
            with self.subTest(status=status):
                self.clock = Clock()
                self.governors = GovernorRegistry(time_fn=self.clock.now, sleep_fn=self.clock.sleep)
                sent, governor = self.a46_status_witness(status=status,
                    retry_after='not-a-deadline', terminal='invalid Retry-After')
                self.assertEqual(len(sent), 1)
                self.assertEqual(governor._pacer._not_before, 1.0,
                                 'malformed retry value must preserve local deadline')

    def test_a46_five_status_failures_exhaust_exact_request_budget(self):
        sent, governor = self.a46_status_witness(retry_after=None, failures=5)
        for left, right, minimum in zip(sent, sent[1:], (1.2, 2.0, 4.0, 8.0)):
            self.assertGreaterEqual(right[0]-left[0]+1e-9, minimum)
        self.assertEqual(governor._pacer._not_before, sent[-1][0]+16.0)

    def test_a46_non_saturation_statuses_keep_exact_error_and_budget(self):
        # Terminal 403/404 classification is pinned separately by A51.
        for status in (503,):
            with self.subTest(status=status):
                self.a46_status_witness(status=status, retry_after=None, failures=1)

    def test_a46_validation_status_defers_without_implicit_retry(self):
        directory = Path(self.temp.name)/'status-validation'
        fault = HTTPError(stockanalysis_url('TEST', 'Max'), 429, 'busy',
                          {'Retry-After': '10'}, None)
        self.addCleanup(fault.close)
        self.addCleanup(fault.close)
        sender = Mock(side_effect=fault)
        governor = self.governors.provider('stockanalysis.history')
        with tp.offline_policy() as capability, self.assertRaises(HTTPError) as caught:
            stock_validate.fetch_daily_reference_single_request('TEST', 'Max',
                _test_capability=capability, _evidence_dir=directory,
                _authority=self.authority, _clock=lambda: datetime(2026,8,4,17,tzinfo=NY),
                _governors=self.governors, _opener=sender)
        self.assertIs(caught.exception, fault)
        sender.assert_called_once_with(stockanalysis_url('TEST', 'Max'))
        self.assertEqual(governor._pacer._not_before, 10.0)
        audit = inspect_ledger(next(directory.glob('*.jsonl')), require_seal=False)
        self.assertEqual([row['event'] for row in audit['events']], ['decision', 'result'])
        self.assertEqual(audit['events'][1]['payload']['http_error'],
                         {'status':429, 'retry_after':'10', 'attempt':1})
        self.assertEqual(stock_validate._REF_CACHE, {})

    def test_a46_status_ledger_faults_precede_policy_and_stop_retry(self):
        for event in ('decision', 'result'):
            with self.subTest(event=event), tp.offline_policy() as capability:
                context = self.context(capability=capability)
                worker = context.worker('a46-durability')
                governor = self.governors.provider('stockanalysis.history')
                sender = Mock(side_effect=HTTPError(stockanalysis_url('TEST','Max'),
                    429, 'busy', {'Retry-After':'10'}, None))
                self.addCleanup(sender.side_effect.close)
                self.addCleanup(sender.side_effect.close)
                provider = external_sweep.StockAnalysisProvider(opener=sender)
                def fault(kind, stage):
                    if (kind, stage) == (event, 'fsync'):
                        raise OSError('a46 durability failure')
                context.ledger._fault = fault
                with patch.object(governor,'http_backoff') as policy, self.assertRaisesRegex(
                        LedgerError, event+' durability failure'):
                    external_sweep._fetch_with_retry(provider,'TEST',Mock(),5,1,
                        sleep_fn=self.clock.sleep,_fetch_worker=worker)
                self.assertEqual(sender.call_count, 0 if event == 'decision' else 1)
                policy.assert_not_called()
                self.assertEqual(stock_validate._REF_CACHE,{})

    def test_a46_invalid_status_metadata_is_bounded_and_terminal(self):
        duplicate = Message()
        duplicate['Retry-After'] = '10'
        duplicate['Retry-After'] = '20'
        cases = ((429, duplicate, 'ambiguous'),
                 (429, {'Retry-After':'10','retry-after':'20'}, 'ambiguous'),
                 (429, {'Retry-After':'x'*257}, 'bounded'),
                 (429, {'Retry-After':10}, 'bounded'),
                 (429, {'Retry-After':None}, 'bounded'),
                 (429, {str(index): 'x' for index in range(129)}, 'unsupported'),
                 (429, object(), 'unsupported'),
                 (True, {}, 'status'), (200, {}, 'status'), (10**50, {}, 'status'))
        cases += tuple((503, headers, expected) for status, headers, expected in cases
                       if status == 429)
        for status, headers, expected in cases:
            with self.subTest(status=status, expected=expected), tp.offline_policy() as capability:
                self.clock = Clock()
                self.governors = GovernorRegistry(time_fn=self.clock.now, sleep_fn=self.clock.sleep)
                context = self.context(capability=capability)
                worker = context.worker('a46-metadata')
                fault = HTTPError(stockanalysis_url('TEST','Max'),status,'busy',headers,None)
                self.addCleanup(fault.close)
                self.addCleanup(fault.close)
                sender = Mock(side_effect=fault)
                retained = labels.stockanalysis_request(worker,'http.stockanalysis.validation','TEST','Max')
                governor = self.governors.provider('stockanalysis.history')
                original = governor.http_backoff
                def backoff(*args, **kwargs):
                    events = inspect_ledger(context.ledger.path, require_seal=False)['events']
                    self.assertEqual([row['event'] for row in events], ['decision', 'result'],
                                     'metadata outcome must be durable before backoff')
                    return original(*args, **kwargs)
                with (patch.object(governor,'http_backoff', side_effect=backoff) as policy,
                      patch.object(governor,'saturate', wraps=governor.saturate) as saturated):
                    with self.assertRaisesRegex(AuthorityError,expected):
                        labels.guarded_stockanalysis_attempt(worker,'http.stockanalysis.validation',
                            'TEST','Max',sender,_logical_request=retained)
                    count = 1 if status in (429, 503) else 0
                    self.assertEqual(saturated.call_count, count,
                                     'malformed metadata must preserve saturation')
                    self.assertEqual(policy.call_count, count)
                    self.assertEqual(governor._pacer._not_before, float(count),
                                     'malformed metadata must preserve local deadline')
                sender.assert_called_once()
                with self.assertRaisesRegex(RequestRefused,'logical request is terminal'):
                    labels.guarded_stockanalysis_attempt(worker,'http.stockanalysis.validation',
                        'TEST','Max',sender,_logical_request=retained)
                sender.assert_called_once()
                payload = inspect_ledger(context.ledger.path,require_seal=False)['events'][1]['payload']
                self.assertEqual(payload['error_type'],'HTTPError')
                self.assertIn(expected,payload['http_error']['metadata_error'])
                self.assertLess(len(json.dumps(payload['http_error'])),256)

    def test_a46_header_observer_has_no_authority_and_fault_is_terminal(self):
        import fetch_ibkr_bridge as bridge
        denied, captured = [], []
        constructor = labels.stockanalysis_request
        directory = Path(self.temp.name)/'header-observer-root'
        def capture(worker, *args):
            captured.append(worker)
            return constructor(worker, *args)
        class Headers:
            def get_all(_self, *_args):
                self.assertIsNone(bridge.current_worker())
                try:
                    constructor(captured[0],'http.stockanalysis.validation','TEST','Max')
                except AuthorityError as exc:
                    denied.append(str(exc))
                raise RequestCancelled('a46 header cancelled')
        with tp.offline_policy() as capability:
            fault = HTTPError(stockanalysis_url('TEST','Max'),429,'busy',Headers(),None)
            self.addCleanup(fault.close)
            self.addCleanup(fault.close)
            sender = Mock(side_effect=fault)
            with patch.object(labels,'stockanalysis_request',side_effect=capture), self.assertRaisesRegex(
                    RequestCancelled,'a46 header cancelled'):
                stock_validate.fetch_daily_reference_single_request('TEST','Max',
                    _test_capability=capability,_evidence_dir=directory,
                    _authority=self.authority,_clock=lambda: datetime(2026,8,4,17,tzinfo=NY),
                    _governors=self.governors,_opener=sender)
            sender.assert_called_once()
            self.assertEqual(len(captured),1)
            payload = inspect_ledger(next(directory.glob('*.jsonl')),require_seal=False)['events'][1]['payload']
            self.assertEqual(payload['error_type'],'HTTPError')
            self.assertEqual(payload['http_error']['metadata_error'],'HTTP header observer failed')
            self.assertEqual(denied,['observer has no operation authority'],
                             'header callback must not borrow the captured worker')

    def a50_bad_header_witness(self, status, headers, message, *, saturation=1):
        self.clock = Clock()
        self.governors = GovernorRegistry(time_fn=self.clock.now, sleep_fn=self.clock.sleep)
        governor = self.governors.provider('stockanalysis.history')
        with tp.offline_policy() as capability:
            context = self.context(capability=capability)
            worker = context.worker('a50-bad-header')
            fault = HTTPError(stockanalysis_url('TEST', 'Max'), status, 'busy', headers, None)
            self.addCleanup(fault.close)
            self.addCleanup(fault.close)
            sender = Mock(side_effect=fault)
            with patch.object(governor, 'saturate', wraps=governor.saturate) as saturated:
                with self.assertRaisesRegex(AuthorityError, message) as caught:
                    labels.guarded_stockanalysis_attempt(worker, 'http.stockanalysis.validation',
                        'TEST', 'Max', sender)
                chain, pending = set(), [caught.exception]
                while pending:
                    error = pending.pop()
                    if error is None or id(error) in chain:
                        continue
                    chain.add(id(error))
                    pending.extend((error.__cause__, error.__context__))
                self.assertIn(id(fault), chain, 'original HTTP fault must remain in exception chain')
                self.assertEqual(saturated.call_count, saturation, 'bad header saturation missing')
                self.assertEqual(governor._pacer._not_before, 1.0, 'bad header local deadline missing')
            sender.assert_called_once()
            events = inspect_ledger(context.ledger.path, require_seal=False)['events']
            self.assertEqual([row['event'] for row in events], ['decision', 'result'])
            self.assertEqual(events[1]['payload']['error_type'], 'HTTPError')
            self.assertEqual(stock_validate._REF_CACHE, {})

    def test_a63_non_saturating_floor_inverses(self):
        from fetch_governors import Governor
        from fetch_run_context import LogicalRequest
        for owner, name, old, new, headers, message in (
            (Governor, 'http_backoff',
             'if saturated or (type(status) is int and (status in (408, 429) or 500 <= status <= 599)):',
             'if saturated:', {'Retry-After': 'soon'}, 'invalid Retry-After'),
            (LogicalRequest, '_execute',
             'if type(status) is int and (status in (408, 429) or 500 <= status <= 599):',
             'if status in (429, 503):', {'Retry-After': 10}, 'bounded'),
        ):
            with self.subTest(method=name):
                mutant = _source_inverse(getattr(owner, name), old, new)
                with patch.object(owner, name, mutant):
                    with self.assertRaisesRegex(AssertionError, 'bad header local deadline missing'):
                        self.a50_bad_header_witness(408, headers, message, saturation=0)
                print('A63_LOCAL_FLOOR_INVERSE_RED', name)

    def test_a63_non_saturating_retryable_bad_headers_keep_local_floor(self):
        for status in (408, 500, 502, 599):
            for headers, message in (({'Retry-After': 'soon'}, 'invalid Retry-After'),
                                     ({'Retry-After': 10}, 'bounded')):
                with self.subTest(status=status, headers=headers):
                    self.a50_bad_header_witness(status, headers, message, saturation=0)

    def test_a50_bad_header_local_floor_and_saturation_inverses(self):
        from fetch_governors import Governor
        from fetch_run_context import LogicalRequest
        cases = (
            (Governor, 'http_backoff', 'if saturated:\n            self.saturate()',
             'if saturated:\n            pass', {'Retry-After': 'soon'}, 'invalid Retry-After',
             'bad header saturation missing'),
            (Governor, 'http_backoff', 'self._pacer.defer(local)', 'pass',
             {'Retry-After': 'soon'}, 'invalid Retry-After', 'bad header local deadline missing'),
            (LogicalRequest, '_execute', 'if type(status) is int and (status in (408, 429) or 500 <= status <= 599):',
             'if False:', {'Retry-After': 10}, 'bounded', 'bad header saturation missing'),
        )
        for owner, name, old, new, headers, message, reason in cases:
            for status in (429, 503):
                with self.subTest(method=name, status=status, reason=reason):
                    self.a50_bad_header_witness(status, headers, message)
                    mutant = _source_inverse(getattr(owner, name), old, new)
                    with patch.object(owner, name, mutant), self.assertRaisesRegex(AssertionError, reason):
                        self.a50_bad_header_witness(status, headers, message)
                    print('A50_INVERSE_RED', name, status, reason)

    def test_a50_malformed_header_keeps_prior_deadline_and_retry_ordinal(self):
        for status in (429, 503):
            with self.subTest(status=status):
                self.clock = Clock()
                self.governors = GovernorRegistry(time_fn=self.clock.now, sleep_fn=self.clock.sleep)
                governor = self.governors.provider('stockanalysis.history')
                with patch.object(governor, 'saturate', wraps=governor.saturate) as saturated:
                    with self.assertRaisesRegex(AuthorityError, 'invalid Retry-After'):
                        governor.http_backoff(status, retry_after='soon', attempt=3)
                    self.assertEqual(governor._pacer._not_before, 4.0)
                    governor._pacer.defer(100.0)
                    with self.assertRaisesRegex(AuthorityError, 'invalid Retry-After'):
                        governor.http_backoff(status, retry_after='soon', attempt=4)
                    self.assertEqual(governor._pacer._not_before, 100.0,
                                     'local floor must not shorten an existing server deadline')
                    self.assertEqual(saturated.call_count, 2)

    def test_a46_header_observer_removal_inverse_is_red(self):
        from fetch_run_context import LogicalRequest
        mutant = _source_inverse(LogicalRequest._execute,
            'without_authority(http_error_evidence)(error)', 'http_error_evidence(error)')
        with patch.object(LogicalRequest,'_execute',mutant), self.assertRaisesRegex(
                AssertionError,'header callback must not borrow'):
            self.test_a46_header_observer_has_no_authority_and_fault_is_terminal()
        print('A46_INVERSE_RED header-observer')

    def test_a46_http_policy_removal_and_evidence_inverses_are_red(self):
        from fetch_run_context import LogicalRequest
        hook = ('governor.http_backoff(http_error["status"],\n'
                '                    retry_after=http_error["retry_after"], attempt=http_attempt,\n'
                '                    max_wait_s=60.0)')
        cases = (
            ('hook',hook,'None','every HTTP status must reach',{}),
            ('ordinal','retry_after=http_error["retry_after"], attempt=http_attempt,',
             'retry_after=http_error["retry_after"], attempt=1,','HTTP retry ordinal',{}),
            ('evidence','result["http_error"] = http_error','pass',
             'actual HTTP status evidence',{}),
            ('deadline','retry_after=http_error["retry_after"]','retry_after=None',
             'server policy must terminate',dict(retry_after='120',terminal='wait budget')),
        )
        for label, old, new, reason, options in cases:
            with self.subTest(mutant=label):
                self.clock = Clock()
                self.governors = GovernorRegistry(time_fn=self.clock.now,sleep_fn=self.clock.sleep)
                mutant = _source_inverse(LogicalRequest._execute,old,new)
                with patch.object(LogicalRequest,'_execute',mutant), self.assertRaisesRegex(AssertionError,reason):
                    self.a46_status_witness(**options)
                print('A46_INVERSE_RED',label)

    def a51_retry_status_witness(self, status):
        self.clock = Clock()
        self.governors = GovernorRegistry(time_fn=self.clock.now, sleep_fn=self.clock.sleep)
        raw = json.dumps({'data': [self.row('2026-08-03')]}).encode()
        fault = HTTPError(stockanalysis_url('TEST', 'Max'), status, 'status classification', {}, None)
        self.addCleanup(fault.close)
        self.addCleanup(fault.close)
        sender = Mock(side_effect=[fault, raw])
        with tp.offline_policy() as capability:
            context = self.context(capability=capability)
            worker = context.worker('a51-status')
            provider = external_sweep.StockAnalysisProvider(opener=sender)
            caught, result = None, None
            try:
                result = external_sweep._fetch_with_retry(provider, 'TEST', Mock(), 5, 1,
                    sleep_fn=self.clock.sleep, _fetch_worker=worker)
            except Exception as exc:
                caught = exc
            retryable = status in (408, 429) or 500 <= status <= 599
            self.assertEqual(sender.call_count, 2 if retryable else 1,
                             'nonretryable HTTP status must stop after one send')
            if retryable:
                self.assertIsNone(caught)
                self.assertEqual(result[1], 2)
            elif status == 403:
                self.assertIsInstance(caught, RequestRefused, '403 must stop the enclosing run')
                self.assertIs(caught.__cause__, fault)
                self.assertIn('provider blocked', str(caught))
            else:
                self.assertIs(caught, fault, 'terminal HTTP error must retain identity')
                self.assertEqual(caught.attempts, 1)
            events = inspect_ledger(context.ledger.path, require_seal=False)['events']
            self.assertEqual([row['event'] for row in events], ['decision', 'result'] * sender.call_count)
            self.assertEqual(len({row['logical_id'] for row in events}), 1)
            self.assertEqual(events[1]['payload']['http_error']['status'], status)

    def test_a51_http_status_retry_classification(self):
        for status in (300, 301, 302, 303, 307, 308, 400, 401, 403, 404, 410,
                       408, 429, 500, 503, 599):
            with self.subTest(status=status):
                self.a51_retry_status_witness(status)

    def test_a51_provider_block_stops_ticker_and_bank(self):
        for root in ('ticker', 'bank'):
            with self.subTest(root=root):
                self.a45_direct_terminal_witness(root, HTTPError, http_status=403)

    def test_a51_retryable_503_exhausts_one_logical_request(self):
        sent, governor = self.a46_status_witness(status=503, retry_after=None, failures=5)
        self.assertEqual(len(sent), 5)
        self.assertEqual(governor._pacer._not_before, sent[-1][0] + 16.0)

    def test_a51_retry_classification_inverses(self):
        for old, new, status, reason in (
            ('if isinstance(exc, HTTPError):', 'if False:', 302,
             'nonretryable HTTP status must stop after one send'),
        ):
            with self.subTest(status=status):
                mutant = _source_inverse(external_sweep._fetch_with_retry, old, new)
                with patch.object(external_sweep, '_fetch_with_retry', mutant), self.assertRaisesRegex(
                        AssertionError, reason):
                    self.a51_retry_status_witness(status)
                print('A51_INVERSE_RED', status)

    def test_a62_sweep_403_conversion_is_mutually_backstopped(self):
        # Shared StockAnalysis conversion now precedes the generic sweep backstop.
        mutant = _source_inverse(external_sweep._fetch_with_retry,
                                 'if exc.code == 403:', 'if False:')
        with patch.object(external_sweep, '_fetch_with_retry', mutant):
            self.a51_retry_status_witness(403)
        print('A62_SWEEP_CONVERSION_INVERSE_BACKSTOPPED_BY_SHARED_POLICY')

    def a52_default_call(self, receiver):
        with tp.offline_policy() as capability, patch.object(
                urllib.request.OpenerDirector, 'open', receiver):
            return stock_validate.fetch_daily_reference_single_request('TEST', 'Max',
                _test_capability=capability, _evidence_dir=Path(self.temp.name) / 'a52-ledgers',
                _authority=self.authority, _clock=lambda: datetime(2026, 8, 4, 17, tzinfo=NY),
                _governors=self.governors)

    def test_a52_default_transport_is_fixed_verified_and_closes(self):
        raw = json.dumps({'data': [self.row('2026-08-03')]}).encode()
        body = io.BytesIO(raw)
        seen = []
        def receiver(opener, wire, *, timeout):
            self.assertEqual(wire.full_url, stockanalysis_url('TEST', 'Max'))
            self.assertEqual(wire.get_method(), 'GET')
            self.assertEqual(wire.get_header('User-agent'), stock_validate._UA['User-Agent'])
            self.assertGreater(timeout, 0)
            self.assertLessEqual(timeout, 20)
            self.assertFalse(any(isinstance(handler, urllib.request.ProxyHandler)
                                 and handler.proxies for handler in opener.handlers))
            https = [handler for handler in opener.handlers
                     if isinstance(handler, http_transport.DeadlineHTTPSHandler)]
            self.assertEqual(len(https), 1)
            self.assertEqual(https[0]._context.verify_mode, ssl.CERT_REQUIRED)
            self.assertTrue(https[0]._context.check_hostname)
            seen.append(wire.full_url)
            return body
        with patch.object(urllib.request, 'getproxies', side_effect=AssertionError('proxy discovery')):
            result = self.a52_default_call(receiver)
        self.assertEqual(len(seen), 1)
        self.assertEqual(result['2026-08-03'][3], 10.0)
        self.assertTrue(body.closed)
        cached = stock_validate._REF_CACHE[('TEST', 'Max')]
        self.assertEqual(cached['source_bytes_sha256'], hashlib.sha256(raw).hexdigest())

    def test_a52_redirects_do_not_follow_and_close_error_body(self):
        for code in (301, 302, 303, 307, 308):
            with self.subTest(code=code):
                body = Mock(wraps=io.BytesIO(b'not a reference'))
                seen = []
                def receiver(opener, wire, *, timeout):
                    seen.append(wire.full_url)
                    handler = next(h for h in opener.handlers if isinstance(h, http_transport.RefuseRedirect))
                    handler.parent.open = Mock(side_effect=AssertionError('redirect followed'))
                    getattr(handler, 'http_error_' + str(code))(
                        wire, body, code, 'redirect', {'Location': 'https://other.invalid/'})
                with self.assertRaises(HTTPError) as caught:
                    self.a52_default_call(receiver)
                self.assertEqual(caught.exception.code, code)
                self.assertEqual(len(seen), 1)
                body.read.assert_not_called()
                body.read1.assert_not_called()
                body.close.assert_called_once()
                self.assertEqual(stock_validate._REF_CACHE, {})

    def test_a52_bounded_body_slow_drip_and_close_precedence(self):
        for kind in ('oversize', 'slow', 'close', 'read-and-close', 'http-and-close'):
            with self.subTest(kind=kind):
                stock_validate.clear_reference_cache()
                clock, sizes, closes = Clock(), [], []
                fault = AuthorityError('primary read failure')
                class Body:
                    def read1(_self, count):
                        sizes.append(count)
                        if kind == 'read-and-close':
                            raise fault
                        if kind == 'slow':
                            clock.sleep(21)
                        if kind == 'oversize':
                            return b'x' * count
                        return b''
                    def close(_self):
                        closes.append(True)
                        if kind in ('close', 'read-and-close', 'http-and-close'):
                            raise OSError('close failure')
                body = Body()
                http_error, close_error = None, None
                if kind == 'http-and-close':
                    http_error = HTTPError(stockanalysis_url('TEST', 'Max'), 404, 'missing', {}, io.BytesIO(b'error'))
                    close_error = http_error.close
                    # Fault only the close operation under test. Keep a real
                    # cleanup handle, so the fixture itself never leaks.
                    http_error.close = body.close
                    self.addCleanup(close_error)
                def receiver(*args, **kwargs):
                    if kind == 'http-and-close':
                        raise http_error
                    return body
                expected = {'oversize': ResponseQuarantined, 'slow': TimeoutError,
                            'close': OSError, 'read-and-close': AuthorityError,
                            'http-and-close': HTTPError}[kind]
                with patch.object(http_transport.time, 'monotonic', clock.now), self.assertRaises(expected) as caught:
                    self.a52_default_call(receiver)
                self.assertEqual(closes, [True])
                self.assertTrue(all(0 < size <= 64 * 1024 for size in sizes))
                if kind == 'oversize':
                    self.assertEqual(sum(sizes), labels._MAX_HISTORY_BYTES + 1)
                if kind == 'read-and-close':
                    self.assertIs(caught.exception, fault)
                if kind == 'http-and-close':
                    self.assertIs(caught.exception, http_error)
                    close_error()
                self.assertEqual(stock_validate._REF_CACHE, {})

    def test_a52_default_direct_root_terminal_matrix(self):
        for root in ('single', 'series', 'cross', 'library', 'ticker', 'bank'):
            for error_type in (AuthorityError, LedgerError, RequestCancelled, RequestRefused):
                with self.subTest(root=root, error=error_type.__name__):
                    self.a45_direct_terminal_witness(root, error_type, use_default=True)

    def test_a52_dns_late_result_has_no_transport_and_one_slot(self):
        entered, release = threading.Event(), threading.Event()
        calls = []
        def lookup():
            calls.append('lookup')
            entered.set()
            self.assertTrue(release.wait(2), 'test must release its DNS worker')
            return [(2, 1, 6, '', ('127.0.0.1', 443))]
        class TinyDeadline:
            def remaining(_self):
                return .02
        with patch.object(http_transport, '_lookup_addresses', lookup), patch.object(
                http_transport, '_make_socket', side_effect=AssertionError('late DNS connected')):
            try:
                with self.assertRaisesRegex(TimeoutError, 'DNS exceeded'):
                    http_transport.resolve(TinyDeadline())
                self.assertTrue(entered.is_set())
                with self.assertRaisesRegex(AuthorityError, 'still occupied'):
                    http_transport.resolve(TinyDeadline())
            finally:
                release.set()
            self.assertTrue(http_transport._DNS_SLOT.acquire(timeout=2))
            http_transport._DNS_SLOT.release()
        self.assertEqual(calls, ['lookup'])

    def test_a52_deadline_interrupts_owned_socket_and_late_tracking(self):
        clock, sock, late = Clock(), object(), object()
        with patch.object(http_transport.time, 'monotonic', clock.now), patch.object(
                http_transport, '_interrupt_socket') as interrupt:
            with http_transport.Deadline(20) as deadline:
                deadline.track(sock)
                clock.sleep(20)
                deadline._expire()
                interrupt.assert_called_once_with(sock)
                with self.assertRaises(TimeoutError):
                    deadline.track(late)
                self.assertEqual(interrupt.call_args.args, (late,))
            self.assertFalse(deadline._timer.is_alive())
            self.assertEqual(deadline._sockets, [])

    def test_a53_resolved_connect_carries_remaining_budget_and_closes_failure(self):
        address = [(2, 1, 6, '', ('127.0.0.1', 443))]
        clock, timeouts, effects = Clock(), [], []
        class Socket:
            def settimeout(_self, value):
                timeouts.append(value)
            def connect(_self, target):
                effects.append(target)
                clock.sleep(3)
            def close(_self):
                effects.append('closed')
        sock = Socket()
        with (patch.object(http_transport.time, 'monotonic', clock.now),
              patch.object(http_transport, 'resolve', return_value=address),
              patch.object(http_transport, '_make_socket', return_value=sock)):
            with http_transport.Deadline(20) as deadline:
                actual = http_transport.connect_resolved(deadline, ('stockanalysis.com', 443), 20)
                self.assertIs(actual, sock)
                self.assertEqual(timeouts, [20, 17], 'TLS must inherit only the remaining budget')
                self.assertEqual(effects, [('127.0.0.1', 443)])
            sock.close()
            with http_transport.Deadline(2) as deadline, self.assertRaises(TimeoutError):
                http_transport.connect_resolved(deadline, ('stockanalysis.com', 443), 2)
            self.assertEqual(effects[-1], 'closed', 'failed connect must close its owned socket')

    def test_a53_parent_connect_tripwire_and_socket_shutdown_are_retained(self):
        with http_transport.Deadline(20) as deadline:
            connection = http_transport.DeadlineHTTPSConnection('stockanalysis.com',
                deadline=deadline, context=ssl.create_default_context(), timeout=20)
            with self.assertRaisesRegex(AuthorityError, 'offline transport tripwire'):
                connection.connect()
            self.assertIsNone(connection.sock)
        sock = object()
        with patch.object(http_transport.socket.socket, 'shutdown') as shutdown:
            http_transport._interrupt_socket(sock)
            shutdown.assert_called_once_with(sock, http_transport.socket.SHUT_RDWR)

    def test_a56_dns_thread_construction_failure_releases_slot(self):
        slot = threading.BoundedSemaphore(1)
        deadline = Mock()
        deadline.remaining.return_value = 20
        fault = RuntimeError("DNS thread construction failed")
        with (patch.object(http_transport, "_DNS_SLOT", slot),
              patch.object(http_transport.threading, "Thread", side_effect=fault)):
            with self.assertRaises(RuntimeError) as caught:
                http_transport.resolve(deadline)
            self.assertIs(caught.exception, fault)
            released = slot.acquire(blocking=False)
            if released:
                slot.release()
            self.assertTrue(released, "failed construction must not occupy the DNS slot")

    def test_a56_connection_cleanup_preserves_primary_failure(self):
        primary = AuthorityError("primary connect failure")
        sock = Mock()
        sock.connect.side_effect = primary
        sock.close.side_effect = OSError("secondary close failure")
        deadline = Mock()
        deadline.remaining.return_value = 20
        with (patch.object(http_transport, "resolve", return_value=[
                (2, 1, 6, "", ("127.0.0.1", 443))]),
              patch.object(http_transport, "_make_socket", return_value=sock)):
            with self.assertRaises(AuthorityError) as caught:
                http_transport.connect_resolved(deadline, ("stockanalysis.com", 443), 20)
            self.assertIs(caught.exception, primary)
            sock.close.assert_called_once()

    def test_a61_each_receive_uses_remaining_budget_and_owns_expiry(self):
        # Invoke the installed socket methods with a memory receiver, never an OS socket.
        for method, args in (('recv', (10,)), ('recv_into', (bytearray(10),)),
                             ('read', (10,))):
            with self.subTest(method=method):
                clock, budgets = Clock(), []
                primary = TimeoutError('native stalled receive')
                class Socket:
                    timeout = 3
                    def settimeout(self, value):
                        self.timeout = value
                sock = Socket()
                def receive(receiver, *args):
                    budgets.append(receiver.timeout)
                    if len(budgets) == 1:
                        clock.sleep(2.5)
                        return 1 if method == 'recv_into' else b'x'
                    clock.sleep(receiver.timeout)
                    raise primary
                with patch.object(http_transport.time, 'monotonic', clock.now):
                    with http_transport.Deadline(3) as deadline:
                        opener = http_transport.build_opener(deadline)
                        handler, = [h for h in opener.handlers
                                    if isinstance(h, http_transport.DeadlineHTTPSHandler)]
                        socket_class = handler._context.sslsocket_class
                        self.assertTrue(issubclass(socket_class, ssl.SSLSocket))
                        with patch.object(ssl.SSLSocket, method, receive):
                            getattr(socket_class, method)(sock, *args)
                            with self.assertRaises(TimeoutError) as caught:
                                getattr(socket_class, method)(sock, *args)
                        self.assertEqual(clock.now(), 3, 'stalled receive must end at total deadline')
                        self.assertEqual(budgets, [3, .5])
                        self.assertEqual(str(caught.exception), 'StockAnalysis total attempt deadline expired')
                        self.assertIs(caught.exception.__cause__, primary)

    def test_a61_stdlib_recv_routes_through_owned_ssl_read(self):
        # Exercise stdlib recv -> self.read -> SSL-object read without an OS socket.
        clock, budgets = Clock(), []
        with patch.object(http_transport.time, 'monotonic', clock.now):
            with http_transport.Deadline(3) as deadline:
                opener = http_transport.build_opener(deadline)
                handler, = [h for h in opener.handlers
                            if isinstance(h, http_transport.DeadlineHTTPSHandler)]
                socket_class = handler._context.sslsocket_class
                sock = SimpleNamespace(_checkClosed=lambda: None, suppress_ragged_eofs=False)
                def settimeout(value):
                    sock.timeout = value
                    budgets.append(value)
                def ssl_read(size):
                    if clock.now() == 0:
                        clock.sleep(2.5)
                        return b'x'
                    clock.sleep(sock.timeout)
                    raise TimeoutError('native SSL read timed out')
                sock.settimeout = settimeout
                sock._sslobj = SimpleNamespace(read=ssl_read)
                sock.read = lambda *args: socket_class.read(sock, *args)
                self.assertEqual(socket_class.recv(sock, 10), b'x')
                with self.assertRaisesRegex(TimeoutError, 'total attempt deadline expired'):
                    socket_class.recv(sock, 10)
                self.assertEqual(clock.now(), 3)
                self.assertEqual(budgets, [3, 3, .5, .5])

    def test_a61_receive_refuses_late_bytes_and_preserves_early_failure(self):
        for mode in ('late', 'early'):
            with self.subTest(mode=mode):
                clock, sock = Clock(), Mock()
                primary = OSError('early receive failure')
                def receive(receiver, *args):
                    clock.sleep(3 if mode == 'late' else 1)
                    if mode == 'early':
                        raise primary
                    return b'late'
                with patch.object(http_transport.time, 'monotonic', clock.now):
                    with http_transport.Deadline(3) as deadline:
                        opener = http_transport.build_opener(deadline)
                        handler, = [h for h in opener.handlers
                                    if isinstance(h, http_transport.DeadlineHTTPSHandler)]
                        socket_class = handler._context.sslsocket_class
                        with patch.object(ssl.SSLSocket, 'read', receive):
                            with self.assertRaises(OSError) as caught:
                                socket_class.read(sock, 10)
                        sock.settimeout.assert_called_once_with(3)
                        if mode == 'early':
                            self.assertIs(caught.exception, primary)
                        else:
                            self.assertEqual(str(caught.exception), 'StockAnalysis total attempt deadline expired')

    def test_a61_ambient_exception_does_not_hide_close_failure(self):
        class Response(io.BytesIO):
            def close(self):
                super().close()
                raise OSError('new response close failure')
        response = Response(json.dumps({'data': [self.row('2026-08-03')]}).encode())
        try:
            raise ValueError('unrelated handled caller exception')
        except ValueError:
            with self.assertRaisesRegex(OSError, 'new response close failure'):
                self.a52_default_call(lambda *args, **kwargs: response)
        self.assertTrue(response.closed)
        self.assertEqual(stock_validate._REF_CACHE, {})

    def test_a62_default_403_stops_all_direct_roots(self):
        for root in ('single', 'series', 'cross', 'library', 'ticker', 'bank'):
            with self.subTest(root=root):
                self.a45_direct_terminal_witness(root, HTTPError, http_status=403, use_default=True)

    def a61_stall_witness(self):
        clock = Clock()
        class Socket:
            timeout = 3
            def settimeout(self, value):
                self.timeout = value
        sock = Socket()
        def receive(receiver, *args):
            clock.sleep(receiver.timeout)
            raise TimeoutError('stalled native read')
        with patch.object(http_transport.time, 'monotonic', clock.now):
            with http_transport.Deadline(3) as deadline:
                opener = http_transport.build_opener(deadline)
                handler, = [h for h in opener.handlers
                            if isinstance(h, http_transport.DeadlineHTTPSHandler)]
                socket_class = handler._context.sslsocket_class
                clock.sleep(2.5)
                with patch.object(ssl.SSLSocket, 'read', receive):
                    with self.assertRaises(TimeoutError):
                        socket_class.read(sock, 10)
                self.assertEqual(clock.now(), 3, 'receive rearm must bound the stall')

    def test_a61_deadline_and_close_inverses(self):
        self.a61_stall_witness()
        for owner, name, old, new, witness, reason in (
            (http_transport, '_receive_before_deadline',
             'sock.settimeout(deadline.remaining())', 'pass',
             self.a61_stall_witness, 'receive rearm must bound the stall'),
            (http_transport, 'build_opener', 'context.sslsocket_class = AttemptSocket',
             'pass', self.a61_stall_witness, 'receive rearm must bound the stall'),
            (labels, 'guarded_stockanalysis_attempt', 'primary = False',
             'primary = __import__("sys").exc_info()[0] is not None',
             self.test_a61_ambient_exception_does_not_hide_close_failure, 'OSError not raised'),
        ):
            with self.subTest(method=name):
                mutant = _source_inverse(getattr(owner, name), old, new)
                with patch.object(owner, name, mutant), self.assertRaisesRegex(AssertionError, reason):
                    witness()
                print('A61_INVERSE_RED', name)

    def test_a62_shared_403_conversion_inverses(self):
        mutant = _source_inverse(labels.guarded_stockanalysis_attempt,
                                 'if error.code == 403:', 'if False:')
        for root in ('single', 'series', 'cross', 'library', 'ticker', 'bank'):
            with self.subTest(root=root):
                with patch.object(labels, 'guarded_stockanalysis_attempt', mutant):
                    if root in ('ticker', 'bank'):
                        self.a45_direct_terminal_witness(root, HTTPError, http_status=403, use_default=True)
                        print('A62_SHARED_CONVERSION_INVERSE_BACKSTOPPED', root)
                    # For sweep roots only, remove its separate documented backstop too.
                    sweep_mutant = _source_inverse(external_sweep._fetch_with_retry,
                                                   'if exc.code == 403:', 'if False:')
                    with patch.object(external_sweep, '_fetch_with_retry', sweep_mutant):
                        with self.assertRaisesRegex(AssertionError, '403 must stop the enclosing run'):
                            self.a45_direct_terminal_witness(root, HTTPError, http_status=403, use_default=True)
                print('A62_SHARED_CONVERSION_INVERSE_RED', root)

    def test_a62_403_durability_precedes_conversion(self):
        for root in ('single', 'series', 'cross', 'library', 'ticker', 'bank'):
            for stage in ('decision', 'result'):
                with self.subTest(root=root, stage=stage):
                    self.a45_direct_terminal_witness(root, HTTPError, http_status=403,
                                                   use_default=True, fault_stage=stage)

    def a64_transport_guard_witness(self, case):
        deadline = Mock()
        deadline.remaining.return_value = 20
        if case == 'host':
            with self.assertRaisesRegex(AuthorityError, 'TLS host differs'):
                http_transport.DeadlineHTTPSConnection('example.com', deadline=deadline,
                                                       context=ssl.create_default_context())
        elif case in ('tunnel', 'tls-rearm'):
            connection = http_transport.DeadlineHTTPSConnection('stockanalysis.com', deadline=deadline,
                                                               context=ssl.create_default_context())
            connection.sock = Mock()
            with patch.object(http_transport.http.client.HTTPSConnection, 'connect') as parent:
                if case == 'tunnel':
                    connection._tunnel_host = 'proxy.invalid'
                    with self.assertRaisesRegex(AuthorityError, 'proxy tunnel'):
                        connection.connect()
                    parent.assert_not_called()
                else:
                    deadline.remaining.side_effect = [20, 7]
                    connection.connect()
                    connection.sock.settimeout.assert_called_once_with(7)
        elif case == 'dns-count':
            class Thread:
                def __init__(_self, *, target, args, **kwargs):
                    _self.run = lambda: target(*args)
                def start(_self):
                    _self.run()
            with (patch.object(http_transport.threading, 'Thread', Thread),
                  patch.object(http_transport, '_DNS_SLOT', threading.BoundedSemaphore(1)),
                  patch.object(http_transport, '_lookup_addresses', return_value=[])):
                with self.assertRaisesRegex(AuthorityError, 'address count'):
                    http_transport.resolve(deadline)
                self.assertTrue(http_transport._DNS_SLOT.acquire(blocking=False))
                http_transport._DNS_SLOT.release()
        elif case == 'dns-context':
            import contextvars
            marker = contextvars.ContextVar('a64-caller-context', default='empty')
            token = marker.set('caller-authority')
            seen = []
            class Thread:
                def __init__(_self, *, target, args, **kwargs):
                    _self.run = lambda: target(*args)
                def start(_self):
                    _self.run()
            def lookup():
                seen.append(marker.get())
                return [(2, 1, 6, '', ('127.0.0.1', 443))]
            try:
                with (patch.object(http_transport.threading, 'Thread', Thread),
                      patch.object(http_transport, '_DNS_SLOT', threading.BoundedSemaphore(1)),
                      patch.object(http_transport, '_lookup_addresses', lookup)):
                    http_transport.resolve(deadline)
                self.assertEqual(seen, ['empty'], 'resolver must not inherit caller context')
            finally:
                marker.reset(token)
        else:
            addresses = [(2, 1, 6, '', ('127.0.0.1', 443))]
            target = ('stockanalysis.com', 443)
            expected = {'target': 'connection target', 'dns-family': 'address is unsupported',
                        'dns-port': 'DNS port differs'}[case]
            if case == 'target':
                target = ('other.invalid', 443)
            elif case == 'dns-family':
                addresses = [(999, 1, 6, '', ('127.0.0.1', 443))]
            else:
                addresses = [(2, 1, 6, '', ('127.0.0.1', 444))]
            with (patch.object(http_transport, 'resolve', return_value=addresses),
                  patch.object(http_transport, '_make_socket') as opened):
                with self.assertRaisesRegex(AuthorityError, expected):
                    http_transport.connect_resolved(deadline, target, 20)
                opened.assert_not_called()

    def a64_injection_witness(self, validation):
        context = self.context()
        worker = context.worker('a64-no-test-capability')
        # Isolate the early check from later admission backstops. No sender runs.
        with patch.object(labels, 'stockanalysis_bounds', side_effect=AssertionError(
                'injection passed early capability barrier')) as bounds:
            with self.assertRaisesRegex(RequestRefused, 'offline policy'):
                if validation:
                    stock_validate.fetch_daily_reference('TEST', 'Max',
                        _fetch_worker=worker, _opener=Mock())
                else:
                    labels.guarded_stockanalysis_attempt(worker, 'http.stockanalysis.validation',
                                                         'TEST', 'Max', Mock())
            bounds.assert_not_called()
        self.assertEqual(context.ledger.path.read_bytes(), b'')

    def a64_cancel_witness(self):
        cancelled, reads, closed = threading.Event(), [], []
        class Response:
            def read1(_self, size):
                reads.append(size)
                cancelled.set()
                return b'{"data":[]}' if len(reads) == 1 else b''
            def close(_self):
                closed.append(True)
        with tp.offline_policy() as capability:
            context = self.context(capability=capability)
            with patch.object(urllib.request.OpenerDirector, 'open', return_value=Response()):
                caught = None
                try:
                    labels.guarded_stockanalysis_attempt(context.worker('a64-cancel'),
                        'http.stockanalysis.validation', 'TEST', 'Max', cancel=cancelled)
                except RequestCancelled as error:
                    caught = error
            self.assertEqual(len(reads), 1, 'cancel must stop before another body read')
            self.assertIsInstance(caught, RequestCancelled)
            self.assertEqual(closed, [True])
            self.assertEqual(stock_validate._REF_CACHE, {})

    def test_a64_guard_witnesses(self):
        for case in ('target', 'host', 'tunnel', 'tls-rearm', 'dns-count',
                     'dns-family', 'dns-port', 'dns-context'):
            with self.subTest(case=case):
                self.a64_transport_guard_witness(case)
        self.a64_injection_witness(False)
        self.a64_injection_witness(True)
        self.a64_cancel_witness()

    def test_a64_guard_inverses(self):
        cases = (
            (http_transport, 'connect_resolved',
             'if address != (_HOST, 443) or source_address is not None:', 'if False:', 'target'),
            (http_transport.DeadlineHTTPSConnection, '__init__',
             'if self.host != _HOST or self.port != 443:', 'if False:', 'host'),
            (http_transport.DeadlineHTTPSConnection, 'connect',
             'if self._tunnel_host:', 'if False:', 'tunnel'),
            (http_transport.DeadlineHTTPSConnection, 'connect',
             'self.sock.settimeout(self._deadline.remaining())', 'pass', 'tls-rearm'),
            (http_transport, 'resolve',
             'if not isinstance(addresses, list) or not 1 <= len(addresses) <= 32:', 'if False:', 'dns-count'),
            (http_transport, 'connect_resolved',
             'if family not in (socket.AF_INET, socket.AF_INET6) or kind != socket.SOCK_STREAM or protocol != socket.IPPROTO_TCP:',
             'if False:', 'dns-family'),
            (http_transport, 'connect_resolved',
             'if not isinstance(target, tuple) or len(target) not in (2, 4) or target[1] != 443:',
             'if False:', 'dns-port'),
            (http_transport, 'resolve', 'target=contextvars.Context().run',
             'target=contextvars.copy_context().run', 'dns-context'),
        )
        for owner, name, old, new, case in cases:
            with self.subTest(case=case):
                function = getattr(owner, name)
                # Preserve __class__ for zero-argument super in class methods.
                source = textwrap.dedent(inspect.getsource(function))
                source = source.replace('super()', 'super(DeadlineHTTPSConnection, self)')
                self.assertEqual(source.count(old), 1)
                # Keep real module globals so the witness's fake resolver is retained.
                mutant = _source_inverse(function, textwrap.dedent(inspect.getsource(function)),
                                         source.replace(old, new, 1))
                with patch.object(owner, name, mutant):
                    with self.assertRaises(AssertionError):
                        self.a64_transport_guard_witness(case)
                print('A64_GUARD_INVERSE_RED', case)
        for validation, owner, name, guard in (
            (False, labels, 'guarded_stockanalysis_attempt',
             'if ((context.test_capability is not None or send is not None)\n            and not is_live_capability(context.test_capability)):'),
            (True, stock_validate, 'fetch_daily_reference',
             'if ((context.test_capability is not None or _opener is not None)\n                and not is_live_capability(context.test_capability)):'),
        ):
            mutant = _source_inverse(getattr(owner, name), guard, 'if False:')
            with patch.object(owner, name, mutant):
                with self.assertRaisesRegex(AssertionError, 'injection passed early capability barrier'):
                    self.a64_injection_witness(validation)
            print('A64_INJECTION_GUARD_INVERSE_RED', name)
        mutant = _source_inverse(labels.guarded_stockanalysis_attempt,
            'if cancel is not None and cancel.is_set():', 'if False:')
        with patch.object(labels, 'guarded_stockanalysis_attempt', mutant):
            with self.assertRaisesRegex(AssertionError, 'cancel must stop before another body read'):
                self.a64_cancel_witness()
        print('A64_CANCEL_GUARD_INVERSE_RED')

    def test_a60_tls_cleanup_preserves_primary_failure(self):
        primary = AuthorityError('TLS deadline primary failure')
        deadline = Mock()
        deadline.remaining.return_value = 20
        deadline.track.side_effect = primary
        connection = http_transport.DeadlineHTTPSConnection('stockanalysis.com',
            deadline=deadline, context=ssl.create_default_context(), timeout=20)
        with (patch.object(http_transport.http.client.HTTPSConnection, 'connect'),
              patch.object(connection, 'close', side_effect=OSError('TLS close secondary')) as close):
            with self.assertRaises(AuthorityError) as caught:
                connection.connect()
            self.assertIs(caught.exception, primary)
            close.assert_called_once()

    def test_a59_expired_constructor_capability_remains_backstopped(self):
        with tp.offline_policy() as capability:
            context = self.context(capability=capability)
            worker = context.worker('a59-expired')
        with self.assertRaisesRegex(AuthorityError, 'offline policy capability expired'):
            labels.stockanalysis_request(worker, 'http.stockanalysis.validation', 'TEST', 'Max')
        mutant = _source_inverse(labels.stockanalysis_request,
            'if worker.context.test_capability is not None and not is_live_capability(worker.context.test_capability):',
            'if False:')
        with self.assertRaisesRegex(AuthorityError, 'offline policy capability expired'):
            mutant(worker, 'http.stockanalysis.validation', 'TEST', 'Max')
        self.assertEqual(context.ledger.path.read_bytes(), b'')
        print('A59_EXPIRED_CONSTRUCTOR_INVERSE_BACKSTOPPED_BY_REQUEST_ADMISSION')

    def test_a59_dns_completed_after_deadline_is_rejected(self):
        clock = Clock()
        class Deadline:
            def remaining(_self):
                if clock.now() >= 20:
                    raise TimeoutError('late completed DNS')
                return 20 - clock.now()
        class Event:
            def set(_self):
                pass
            def wait(_self, timeout):
                clock.sleep(21)
                return True
        class Thread:
            def __init__(_self, *, target, args, **kwargs):
                _self.run = lambda: target(*args)
            def start(_self):
                _self.run()
        with (patch.object(http_transport, '_DNS_SLOT', threading.BoundedSemaphore(1)),
              patch.object(http_transport.threading, 'Event', Event),
              patch.object(http_transport.threading, 'Thread', Thread),
              patch.object(http_transport, '_lookup_addresses', return_value=[
                  (2, 1, 6, '', ('127.0.0.1', 443))]),
              patch.object(http_transport, '_make_socket') as opened):
            with self.assertRaisesRegex(TimeoutError, 'late completed DNS'):
                http_transport.resolve(Deadline())
            opened.assert_not_called()

    def test_a59_deadline_inverses_have_observable_effects(self):
        cases = (
            (http_transport.Deadline, 'remaining',
             'if self._closed or self._expired or value <= 0:', 'if False:',
             'test_a52_deadline_interrupts_owned_socket_and_late_tracking'),
            (http_transport.Deadline, '_expire', '_interrupt_socket(sock)', 'pass',
             'test_a52_deadline_interrupts_owned_socket_and_late_tracking'),
            (http_transport.Deadline, '__exit__', 'self._sockets.clear()', 'pass',
             'test_a52_deadline_interrupts_owned_socket_and_late_tracking'),
            (http_transport, 'connect_resolved',
             '# HTTPSConnection\'s TLS handshake inherits this *remaining* budget.\n            sock.settimeout(deadline.remaining())',
             '# Incorrectly restart the TLS budget.\n            sock.settimeout(20)',
             'test_a53_resolved_connect_carries_remaining_budget_and_closes_failure'),
            (http_transport, 'resolve',
             'deadline.remaining()  # A late result is never permission to connect.', 'pass',
             'test_a59_dns_completed_after_deadline_is_rejected'),
        )
        for owner, name, old, new, witness in cases:
            with self.subTest(check=name):
                mutant = _source_inverse(getattr(owner, name), old, new)
                with patch.object(owner, name, mutant):
                    with self.assertRaises(AssertionError):
                        getattr(self, witness)()
                print('A59_DEADLINE_INVERSE_RED', name)

    def test_a59_exact_worker_constructor_and_warm_cache_barriers(self):
        with tp.offline_policy() as capability:
            context = self.context(capability=capability)
            real = context.worker('a59-worker')
            impostor = SimpleNamespace(context=context, worker_id=real.worker_id,
                                       request=Mock())
            for call in (
                lambda: labels.stockanalysis_request(impostor,
                    'http.stockanalysis.validation', 'TEST', 'Max'),
                lambda: stock_validate.fetch_daily_reference('TEST', 'Max',
                    _fetch_worker=impostor, _run_cache={('TEST', 'Max'): {'cached': True}}),
            ):
                with self.assertRaisesRegex(RequestRefused, 'operation worker'):
                    call()
            impostor.request.assert_not_called()
            self.assertEqual(context.ledger.path.read_bytes(), b'')

    def test_a59_exact_worker_barrier_inverses(self):
        with tp.offline_policy() as capability:
            context = self.context(capability=capability)
            real = context.worker('a59-inverse')
            impostor = SimpleNamespace(context=context, worker_id=real.worker_id,
                                       request=Mock(return_value=object()))
            constructor = _source_inverse(labels.stockanalysis_request,
                'if type(worker) is not FetchWorker:', 'if False:')
            retained = constructor(impostor, 'http.stockanalysis.validation', 'TEST', 'Max')
            impostor.request.assert_called_once()
            self.assertIs(retained.request, impostor.request.return_value)
            cache = _source_inverse(stock_validate.fetch_daily_reference,
                'if type(_fetch_worker) is not FetchWorker:', 'if False:')
            value = cache('TEST', 'Max', _fetch_worker=impostor,
                          _run_cache={('TEST', 'Max'): {'cached': True}})
            self.assertEqual(value, {'cached': True}, 'removed identity barrier must expose cache')
            self.assertEqual(context.ledger.path.read_bytes(), b'')

    def test_a54_default_cache_hits_expiry_tamper_fresh_and_admission(self):
        raw = json.dumps({'data': [self.row('2026-08-03')]}).encode()
        calls = []
        def receiver(*args, **kwargs):
            calls.append(True)
            return io.BytesIO(raw)
        first = self.a52_default_call(receiver)
        key = ('TEST', 'Max')
        good = deepcopy(stock_validate._REF_CACHE[key])
        for label, age, fresh, tamper, sends in (
            ('hit', 899, False, False, 0), ('expiry', 900, False, False, 1),
            ('negative', -1, False, False, 1), ('tamper', 1, False, True, 1),
            ('fresh', 1, True, False, 1)):
            with self.subTest(case=label), tp.offline_policy() as capability:
                stock_validate._REF_CACHE[key] = deepcopy(good)
                if tamper:
                    stock_validate._REF_CACHE[key]['source_bytes_sha256'] = '0' * 64
                context = self.context(capability=capability)
                before = len(calls)
                with patch.object(urllib.request.OpenerDirector, 'open', receiver):
                    result = stock_validate.fetch_daily_reference('TEST', 'Max',
                        _now=good['fetched_ts'] + age, _force_fresh=fresh,
                        _fetch_worker=context.worker('a54-cache'))
                self.assertEqual(result, first)
                self.assertEqual(len(calls) - before, sends)
        stock_validate._REF_CACHE[key] = deepcopy(good)
        context = self.context()
        with patch.object(urllib.request.OpenerDirector, 'open') as opened:
            for injected in (None, Mock(side_effect=AssertionError('injected sender called'))):
                with self.subTest(injected=injected is not None), self.assertRaises(RequestRefused):
                    stock_validate.fetch_daily_reference('TEST', 'Max',
                        _now=good['fetched_ts'], _opener=injected,
                        _fetch_worker=context.worker('held'))
            opened.assert_not_called()
        self.assertEqual(context.ledger.path.read_bytes(), b'')
        self.assertEqual(stock_validate._REF_CACHE[key], good)

    def a45_direct_terminal_witness(self, root_name, error_type, *, http_status=None,
                                   use_default=False, fault_stage=None):
        """Fault at raw transport, retaining the real root/guard/ledger path."""
        project = Path(self.temp.name) / (root_name + '-' + error_type.__name__ + (fault_stage or ''))
        bank, logs = project / 'bank', project / 'Run Logs'
        directory = logs / 'ledgers'
        stock_validate.clear_reference_cache()
        fault = (error_type('a45 direct terminal transport fault') if http_status is None else
                 HTTPError(stockanalysis_url('AAA', 'Max'), http_status, 'provider block', {}, None))
        if isinstance(fault, HTTPError):
            self.addCleanup(fault.close)
        sent, reads = [], []

        def opener(url, *args):
            sent.append((url, args))
            if fault_stage is not None and http_status is None:
                return json.dumps({'data': [self.row('2026-08-03')]}).encode()
            raise fault

        def default_open(_opener, wire, *, timeout):
            self.assertEqual(wire.get_method(), 'GET')
            self.assertGreater(timeout, 0)
            self.assertLessEqual(timeout, 20)
            value = opener(wire.full_url, *((20.0,) if sweep else ()))
            return io.BytesIO(value)

        original_create = FetchRunContext.create
        faults = []
        def create(*args, **kwargs):
            context = original_create(*args, **kwargs)
            if fault_stage in ('decision', 'result'):
                def fail(event, stage):
                    if (event, stage) == (fault_stage, 'fsync'):
                        faults.append((event, stage))
                        raise OSError('A57 direct durability fault')
                context.ledger._fault = fail
            return context
        governor = self.governors.provider('stockanalysis.history')
        original_turn = governor.wait_turn
        def wait_turn(*args, **kwargs):
            if fault_stage == 'turn':
                faults.append(('turn', 'acquire'))
                raise fault
            return original_turn(*args, **kwargs)

        def read(_root, ticker, _interval):
            reads.append(ticker)
            return [(datetime(2026, 8, 3), 10.0, 11.0, 9.0, 10.0, 100)]

        def fingerprint(_root, ticker, interval):
            return {'schema_version': stock_validate.ss.INTERVAL_FINGERPRINT_VERSION,
                    'algorithm': 'sha256', 'sha256': 'a' * 64,
                    'ticker': ticker, 'interval': interval, 'present': True,
                    'backfill_incomplete': False, 'month_count': 1,
                    'verified_absent_count': 0}

        def snapshot(_root, ticker):
            reads.append(ticker)
            return {'ticker': ticker, 'conid': 101, 'provider_symbol': ticker,
                    'manifest_fingerprint': 'a' * 64,
                    'stored': {'2026-08-03': 10.0}, 'stored_rows': 1,
                    'stored_first': '2026-08-03', 'stored_last': '2026-08-03',
                    'stored_basis': 'raw', 'basis_actions_applied': []}

        common = {'_evidence_dir': directory, '_authority': self.authority,
                  '_clock': lambda: datetime(2026, 8, 4, 17, tzinfo=NY),
                  '_governors': self.governors}
        sweep = root_name in ('ticker', 'bank')
        with (tp.offline_policy() as capability,
              patch.object(FetchRunContext, 'create', side_effect=create),
              patch.object(governor, 'wait_turn', side_effect=wait_turn),
              patch.object(stock_validate, '_publish_reference_cache') as publish,
              patch.object(stock_validate, 'save_daily_reference') as sidecar,
              patch.object(stock_validate, 'record_cross_validation') as verdict,
              patch.object(external_sweep, 'stored_daily_snapshot', side_effect=snapshot),
              patch.object(external_sweep, '_manifest_unchanged', return_value=True),
              patch.object(external_sweep, '_manifest_record', side_effect=snapshot),
              patch.object(external_sweep, 'write_cache') as cache_write,
              patch.object(external_sweep, 'write_artifact') as report_write,
              patch.object(urllib.request.OpenerDirector, 'open', default_open)):
            common['_test_capability'] = capability
            caught = None
            try:
                if sweep:
                    provider = external_sweep.StockAnalysisProvider(opener=None if use_default else opener,
                        now=lambda: datetime(2026, 8, 4, tzinfo=timezone.utc))
                    options = dict(provider=provider, cache_root=logs/'cache',
                                   attempts=5, backoff=1, sleep_fn=self.clock.sleep,
                                   **common)
                    if root_name == 'ticker':
                        external_sweep.sweep_ticker(bank, 'AAA', pacer=Mock(), **options)
                    else:
                        external_sweep.sweep_bank(bank, ['AAA', 'BBB'],
                            run_logs_root=logs, gate_path=logs/'.lock',
                            now=datetime(2026, 8, 4, tzinfo=timezone.utc),
                            clock=self.clock.now, **options)
                else:
                    common['_opener'] = None if use_default else opener
                    if root_name == 'single':
                        stock_validate.fetch_daily_reference_single_request('AAA', 'Max', **common)
                    elif root_name == 'series':
                        stock_validate.validate_series(bank, 'AAA', '1d', rng='Max',
                            read_fn=read, save_reference=True, **common)
                    elif root_name == 'cross':
                        stock_validate.cross_validate_ticker(bank, 'AAA', '1d', rng='Max',
                            read_fn=read, fingerprint_fn=fingerprint, **common)
                    elif root_name == 'library':
                        stock_validate.revalidate_library(bank,
                            series=[('AAA', '1d'), ('BBB', '1d')], rng='Max',
                            read_fn=read, save_reference=True, **common)
                    else:
                        self.fail('unknown matrix root')
            except Exception as exc:
                caught = exc
        if fault_stage in ('decision', 'result'):
            self.assertIs(type(caught), LedgerError, 'durability fault must stop the root')
            self.assertIn(fault_stage + ' durability failure', str(caught))
            self.assertEqual(faults, [(fault_stage, 'fsync')])
        elif http_status is None:
            self.assertIs(caught, fault, 'terminal error must escape unchanged')
        else:
            self.assertIsInstance(caught, RequestRefused, '403 must stop the enclosing run')
            self.assertIs(caught.__cause__, fault)
        expected_sends = 0 if fault_stage in ('turn', 'decision') else 1
        self.assertEqual(sent, [(stockanalysis_url('AAA', 'Max'), (20.0,) if sweep else ())] * expected_sends)
        self.assertNotIn('BBB', reads, 'terminal fault must stop the batch')
        for sink in (publish, sidecar, verdict, cache_write, report_write):
            sink.assert_not_called()
        self.assertEqual(stock_validate._REF_CACHE, {})
        self.assertFalse((project/'Validation Reference').exists())
        paths = list(directory.glob('*.jsonl'))
        self.assertEqual(len(paths), 1)
        self.assertFalse(paths[0].with_suffix('.seal.json').exists())
        if fault_stage == 'turn':
            self.assertEqual(faults, [('turn', 'acquire')])
            self.assertEqual(paths[0].read_bytes(), b'')
            print('A57_DIRECT_DURABILITY', root_name, 'turn; opens=0 events=0 publish=0')
            return
        audit = inspect_ledger(paths[0], require_seal=False)
        self.assertFalse(audit['verified'])
        events = audit['events']
        self.assertEqual([event['event'] for event in events],
                         ['decision'] if fault_stage == 'decision' else ['decision', 'result'])
        if fault_stage == 'decision':
            self.assertEqual(events[0]['producer_id'], 'http.stockanalysis.external_sweep'
                             if sweep else 'http.stockanalysis.validation')
            print('A57_DIRECT_DURABILITY', root_name, 'decision; opens=0 visible=1 publish=0')
            return
        self.assertEqual(events[0]['attempt_id'], events[1]['attempt_id'])
        self.assertEqual(events[0]['logical_id'], events[1]['logical_id'])
        self.assertEqual({event['producer_id'] for event in events}, {
            'http.stockanalysis.external_sweep' if sweep else 'http.stockanalysis.validation'})
        self.assertEqual(events[1]['payload']['outcome'],
                         'returned' if fault_stage == 'result' and http_status is None else 'error')
        if fault_stage == 'result':
            print('A57_DIRECT_DURABILITY', root_name, 'result; opens=1 visible=2 publish=0')
            return
        self.assertEqual(events[1]['payload']['error_type'], error_type.__name__)
        print('A45_DIRECT_TERMINAL', root_name, error_type.__name__, 'physical=1 ledger=2 seal=0 publish=0')

    def test_a57_default_direct_turn_decision_result_matrix(self):
        for root in ('single', 'series', 'cross', 'library', 'ticker', 'bank'):
            for stage in ('turn', 'decision', 'result'):
                with self.subTest(root=root, stage=stage):
                    self.a45_direct_terminal_witness(root,
                        RequestCancelled if stage == 'turn' else LedgerError,
                        use_default=True, fault_stage=stage)

    def a45_direct_terminal_matrix(self, root_name):
        for error_type in (AuthorityError, LedgerError, RequestCancelled, RequestRefused):
            with self.subTest(root=root_name, error=error_type.__name__):
                self.a45_direct_terminal_witness(root_name, error_type)

    def test_a45_single_root_terminal_matrix(self):
        self.a45_direct_terminal_matrix('single')

    def test_a45_series_root_terminal_matrix(self):
        self.a45_direct_terminal_matrix('series')

    def test_a45_cross_root_terminal_matrix(self):
        self.a45_direct_terminal_matrix('cross')

    def test_a45_library_root_terminal_matrix(self):
        self.a45_direct_terminal_matrix('library')

    def test_a45_ticker_root_terminal_matrix(self):
        self.a45_direct_terminal_matrix('ticker')

    def test_a45_bank_root_terminal_matrix(self):
        self.a45_direct_terminal_matrix('bank')

    def test_a45_direct_terminal_swallowing_inverses_are_red(self):
        terminal = 'if _terminal_reference_error(exc, _fetch_worker):'
        single = ('            result = fetch_daily_reference(\n'
                  '                ticker, rng, _fetch_worker=worker, _opener=_opener,\n'
                  '                _pending_cache=pending_cache)')
        swallowed = ('            try:\n' + textwrap.indent(single, '    ') + '\n'
                     '            except Exception:\n                result = {}')
        sweep = ('except (AuthorityError, LedgerError, RequestCancelled, RequestRefused,\n'
                 '            SpoolError):')
        cases = (
            ('single', stock_validate, 'fetch_daily_reference_single_request', single, swallowed),
            ('series', stock_validate, '_validate_series_body', terminal, 'if False:'),
            ('cross', stock_validate, '_cross_validate_ticker_body', terminal, 'if False:'),
            ('library', stock_validate, '_revalidate_library_body', terminal, 'if False:'),
            ('ticker', external_sweep, '_sweep_ticker_body', sweep, 'except SpoolError:'),
            ('bank', external_sweep, '_sweep_ticker_body', sweep, 'except SpoolError:'),
        )
        for root_name, module, name, old, new in cases:
            with self.subTest(root=root_name):
                mutant = _source_inverse(getattr(module, name), old, new)
                with patch.object(module, name, mutant), self.assertRaisesRegex(
                        AssertionError, 'terminal error must escape unchanged'):
                    self.a45_direct_terminal_witness(root_name, LedgerError)
                print('A45_DIRECT_INVERSE_RED', root_name, name)

    def test_sweep_terminal_ledger_fault_does_not_become_batch_result(self):
        project = Path(self.temp.name) / "terminal"
        bank = project / "Data Bank"
        logs = project / "Run Logs"
        snapshot = {
            "ticker": "TEST", "conid": 101, "provider_symbol": "TEST",
            "manifest_fingerprint": "a" * 64, "stored": {"2026-08-03": 10.0},
            "stored_rows": 1, "stored_first": "2026-08-03",
            "stored_last": "2026-08-03", "stored_basis": "raw",
            "basis_actions_applied": [],
        }
        provider = external_sweep.StockAnalysisProvider(
            opener=lambda *_args: self.fail("transport must stay untouched"))
        with (tp.offline_policy() as capability,
              patch.object(external_sweep, "stored_daily_snapshot",
                           return_value=snapshot),
              patch.object(external_sweep, "_provider_fetch",
                           side_effect=LedgerError("injected ledger failure")),
              patch.object(external_sweep, "write_cache") as cache_write):
            with self.assertRaises(LedgerError):
                external_sweep.sweep_ticker(
                    bank, "TEST", provider=provider,
                    cache_root=logs / "_external_sweep_cache",
                    _test_capability=capability,
                    _evidence_dir=logs / "ledgers",
                    _authority=self.authority,
                    _clock=lambda: datetime(2026, 8, 4, 17, tzinfo=NY),
                    _governors=self.governors)
        cache_write.assert_not_called()
        ledgers = list((logs / "ledgers").glob("*.jsonl"))
        self.assertEqual(len(ledgers), 1)
        self.assertNotIn('"event":"seal"', ledgers[0].read_text(
            encoding="utf-8"))

    def test_failed_validation_seal_preserves_reference_cache_and_sidecar(self):
        raw = json.dumps({"data": [self.row("2026-08-03")]}).encode("utf-8")
        bank = Path(self.temp.name) / "bank"
        bars = [(datetime(2026, 8, 3), 10.0, 11.0, 9.0, 10.0, 100)]
        with (tp.offline_policy() as capability,
              patch.object(FetchRunContext, "seal",
                           side_effect=LedgerError("injected seal failure"))):
            with self.assertRaises(LedgerError):
                stock_validate.validate_series(
                    bank, "TEST", "1d", rng="Max", save_reference=True,
                    read_fn=lambda *_args: bars,
                    _test_capability=capability,
                    _evidence_dir=Path(self.temp.name) / "seal-ledgers",
                    _authority=self.authority,
                    _clock=lambda: datetime(2026, 8, 4, 17, tzinfo=NY),
                    _governors=self.governors, _opener=lambda _url: raw)
        self.assertNotIn(("TEST", "Max"), stock_validate._REF_CACHE)
        self.assertFalse((bank.parent / "Validation Reference").exists())

    def test_force_fresh_batch_preserves_old_cache_until_seal(self):
        old_raw = json.dumps({"data": [self.row("2026-08-03")]}).encode("utf-8")
        new_raw = json.dumps({"data": [self.row("2026-08-03", 20)]}).encode("utf-8")
        options = {
            "_evidence_dir": Path(self.temp.name) / "batch-ledgers",
            "_authority": self.authority,
            "_clock": lambda: datetime(2026, 8, 4, 17, tzinfo=NY),
            "_governors": self.governors,
        }
        with tp.offline_policy() as capability:
            options["_test_capability"] = capability
            stock_validate.fetch_daily_reference_single_request(
                "TEST", "Max", _opener=lambda _url: old_raw, **options)
            previous = deepcopy(stock_validate._REF_CACHE[("TEST", "Max")])
            seen = []
            bars = [(datetime(2026, 8, 3), 20.0, 21.0, 19.0, 20.0, 100)]
            with patch.object(FetchRunContext, "seal",
                              side_effect=LedgerError("injected seal failure")):
                with self.assertRaises(LedgerError):
                    stock_validate.revalidate_library(
                        "unused-bank", series=[("TEST", "1d"),
                                                ("TEST", "1d")],
                        rng="Max", force_fresh=True,
                        read_fn=lambda *_args: bars,
                        _opener=lambda url: seen.append(url) or new_raw,
                        **options)
        self.assertEqual(seen, [stockanalysis_url("TEST", "Max")])
        self.assertEqual(stock_validate._REF_CACHE[("TEST", "Max")], previous)

    def test_failed_sweep_seal_preserves_existing_cache_bytes(self):
        project = Path(self.temp.name) / "sweep-seal"
        bank = project / "Data Bank"
        logs = project / "Run Logs"
        cache_root = logs / "_external_sweep_cache"
        cache_root.mkdir(parents=True)
        snapshot = {
            "ticker": "TEST", "conid": 101, "provider_symbol": "TEST",
            "manifest_fingerprint": "a" * 64, "stored": {"2026-08-03": 10.0},
            "stored_rows": 1, "stored_first": "2026-08-03",
            "stored_last": "2026-08-03", "stored_basis": "raw",
            "basis_actions_applied": [],
        }
        identity = {key: snapshot[key] for key in
                    ("ticker", "conid", "provider_symbol",
                     "manifest_fingerprint")}
        existing = external_sweep._cache_path(cache_root, identity)
        existing.write_bytes(b"prior last-good cache bytes\n")
        raw = json.dumps({"data": [self.row("2026-08-03")]}).encode("utf-8")
        provider = external_sweep.StockAnalysisProvider(
            opener=lambda _url, _timeout: raw,
            now=lambda: datetime(2026, 8, 4, tzinfo=timezone.utc))
        with (tp.offline_policy() as capability,
              patch.object(external_sweep, "stored_daily_snapshot",
                           return_value=snapshot),
              patch.object(external_sweep, "_manifest_unchanged",
                           return_value=True),
              patch.object(FetchRunContext, "seal",
                           side_effect=LedgerError("injected seal failure")),
              patch.object(external_sweep, "write_cache") as cache_write):
            with self.assertRaises(LedgerError):
                external_sweep.sweep_ticker(
                    bank, "TEST", provider=provider,
                    cache_root=cache_root,
                    _test_capability=capability,
                    _evidence_dir=logs / "ledgers",
                    _authority=self.authority,
                    _clock=lambda: datetime(2026, 8, 4, 17, tzinfo=NY),
                    _governors=self.governors)
        cache_write.assert_not_called()
        self.assertEqual(existing.read_bytes(), b"prior last-good cache bytes\n")

    def test_sweep_roots_hold_and_publish_only_after_seal(self, use_default=False):
        project = Path(self.temp.name) / "project"
        bank = project / "Data Bank"
        logs = project / "Run Logs"
        cache = logs / "_external_sweep_cache"
        seen = []
        provider = external_sweep.StockAnalysisProvider(
            opener=lambda url, timeout: seen.append(url) or b"{}")
        for entry in (
            lambda: external_sweep.sweep_ticker(
                bank, "TEST", provider=provider, cache_root=cache,
                _evidence_dir=logs / "ledgers"),
            lambda: external_sweep.sweep_bank(
                bank, ["TEST"], provider=provider, cache_root=cache,
                run_logs_root=logs, _evidence_dir=logs / "ledgers"),
        ):
            with self.subTest(entry=entry), self.assertRaises(AuthorityError):
                entry()
        self.assertEqual(seen, [])
        self.assertFalse(bank.exists())

        reference = {}
        for offset in range(12):
            year = 2025 + (6 + offset) // 12
            month = (6 + offset) % 12 + 1
            reference[f"{year:04d}-{month:02d}-03"] = (
                100.0, 100.0, 100.0, 100.0, 1000)
        snapshot = {
            "ticker": "TEST", "conid": 101, "provider_symbol": "TEST",
            "manifest_fingerprint": "a" * 64,
            "stored": {day: 100.0 for day in reference},
            "stored_rows": len(reference),
            "stored_first": min(reference), "stored_last": max(reference),
            "stored_basis": "raw", "basis_actions_applied": [],
        }
        raw = json.dumps({"data": [
            {"t": day, "o": row[0], "h": row[1], "l": row[2],
             "c": row[3], "v": row[4]}
            for day, row in reference.items()]}).encode("utf-8")
        responses = self.a55_default_receiver(lambda url: seen.append(url) or raw) if use_default else []
        provider = external_sweep.StockAnalysisProvider(
            opener=None if use_default else lambda url, timeout: seen.append(url) or raw,
            now=lambda: datetime(2026, 8, 4, tzinfo=timezone.utc))
        writes_after_seal = []

        def checked_write(target_cache, bank_root, identity, payload):
            ledgers = sorted((logs / "ledgers").glob("*.jsonl"))
            sealed = [[json.loads(line) for line in path.read_text(
                encoding="utf-8").splitlines()] for path in ledgers]
            matching = [events for events in sealed if events and
                events[0]["operation_id"] ==
                payload["http_attempt"]["operation_id"]]
            self.assertEqual(len(matching), 1)
            writes_after_seal.append(matching[0][-1]["event"] == "seal"
                                     and len(matching[0]) == 3)
            self.assertEqual(payload["http_attempt"]["range"], "Max")
            return cache / "TEST__101.json", payload

        with (tp.offline_policy() as capability,
              patch.object(external_sweep, "stored_daily_snapshot",
                           return_value=snapshot),
              patch.object(external_sweep, "_manifest_unchanged",
                           return_value=True),
              patch.object(external_sweep, "write_cache",
                           side_effect=checked_write)):
            row = external_sweep.sweep_ticker(
                bank, "TEST", provider=provider, cache_root=cache,
                asof=datetime(2026, 8, 4, tzinfo=timezone.utc),
                _test_capability=capability, _evidence_dir=logs / "ledgers",
                _authority=self.authority,
                _clock=lambda: datetime(2026, 8, 4, 17, tzinfo=NY),
                _governors=self.governors)
        self.assertEqual(len(seen), 1)
        self.assertEqual(writes_after_seal, [True])
        self.assertEqual(row["network_requests"], 1)
        self.assertEqual(row["cache_status"], "UNVERIFIED")

        artifact_sealed = []
        def checked_artifact(target, report, **kwargs):
            ledgers = sorted((logs / "ledgers").glob("*.jsonl"))
            events = [json.loads(line) for line in ledgers[-1].read_text(
                encoding="utf-8").splitlines()]
            artifact_sealed.append(events[-1]["event"] == "seal")
            return target

        with (tp.offline_policy() as capability,
              patch.object(external_sweep, "stored_daily_snapshot",
                           return_value=snapshot),
              patch.object(external_sweep, "_manifest_unchanged",
                           return_value=True),
              patch.object(external_sweep, "_manifest_record",
                           return_value=snapshot),
              patch.object(external_sweep, "write_cache",
                           side_effect=checked_write),
              patch.object(external_sweep, "write_artifact",
                           side_effect=checked_artifact)):
            report = external_sweep.sweep_bank(
                bank, ["TEST"], provider=provider, cache_root=cache,
                run_logs_root=logs, gate_path=logs / ".sweep.lock",
                now=datetime(2026, 8, 4, tzinfo=timezone.utc),
                clock=self.clock.now, sleep_fn=self.clock.sleep,
                _test_capability=capability, _evidence_dir=logs / "ledgers",
                _authority=self.authority,
                _clock=lambda: datetime(2026, 8, 4, 17, tzinfo=NY),
                _governors=self.governors)
        self.assertEqual(report["network_requests"], 1)
        self.assertEqual(len(seen), 2)
        self.assertEqual(writes_after_seal, [True, True])
        self.assertEqual(artifact_sealed, [True])
        if use_default:
            self.assertEqual(len(responses), 2)
            self.assertTrue(all(response.closed for response in responses))

    def a55_default_receiver(self, raw_response):
        responses = []
        def physical_open(_opener, wire, *, timeout):
            frame = inspect.currentframe().f_back
            self.assertEqual(frame.f_code.co_qualname,
                             "guarded_stockanalysis_attempt.<locals>.one_send")
            if hasattr(self, "_a58_physical"):
                self._a58_physical(frame)
            self.assertEqual(wire.get_method(), "GET")
            self.assertEqual(wire.full_url, stockanalysis_url("TEST", "Max"))
            self.assertGreater(timeout, 0)
            self.assertLessEqual(timeout, 20)
            response = io.BytesIO(raw_response(wire.full_url))
            responses.append(response)
            return response
        self.enterContext(patch.object(urllib.request.OpenerDirector, "open", physical_open))
        return responses

    def test_a55_default_success_all_six_direct_roots(self):
        self.test_validation_roots_direct_positive_share_guarded_consumer(use_default=True)
        stock_validate.clear_reference_cache()
        self.test_single_request_root_reaches_guarded_attempt(use_default=True)
        stock_validate.clear_reference_cache()
        self.test_sweep_roots_hold_and_publish_only_after_seal(use_default=True)
        print("A55_DEFAULT_DIRECT_SUCCESS six roots; six physical opens; sealed budgets and closed handles")

    def test_a58_default_selection_inverses(self):
        for owner, name, old, new in (
                (labels, 'guarded_stockanalysis_attempt', 'if send is not None:', 'if True:'),
                (stock_validate, 'fetch_daily_reference',
                 'one_send if _opener is not None else None', 'one_send'),
                (external_sweep.StockAnalysisProvider, '_read',
                 'one_send if self.opener is not None else None', 'one_send')):
            with self.subTest(owner=name):
                mutant = _source_inverse(getattr(owner, name), old, new)
                result = unittest.TestResult()
                case = type(self)('test_a55_default_success_all_six_direct_roots')
                with patch.object(owner, name, mutant):
                    case.run(result)
                self.assertFalse(result.wasSuccessful(), 'default selection inverse survived: ' + name)
                self.assertEqual(result.testsRun, 1)
                self.assertEqual(len(result.errors) + len(result.failures), 1)
                print('A58_DEFAULT_SELECTION_INVERSE_RED', name)

    def test_a58_owned_physical_site_binds_both_logical_producers(self):
        inventory = fetch_send_inventory.build_inventory()
        site = next(row for row in inventory['sites'] if row['site_key'] ==
            'engine.fetch_http_labels:guarded_stockanalysis_attempt.one_send:http_opener_open:1')
        pending, sent, results = {}, [], set()
        original_decision, original_result = FetchLedger.decision, FetchLedger.result
        def decision(ledger, ids, payload):
            value = original_decision(ledger, ids, payload)
            pending[threading.get_ident()] = (ids['producer_id'], ids['operation_id'], ids['attempt_id'])
            return value
        def result(ledger, ids, payload):
            value = original_result(ledger, ids, payload)
            results.add((ids['producer_id'], ids['operation_id'], ids['attempt_id']))
            return value
        def physical(frame):
            self.assertEqual(Path(frame.f_code.co_filename).name, 'fetch_http_labels.py')
            self.assertEqual(frame.f_lineno, site['line'], 'physical source line must match inventory')
            self.assertEqual(site['disposition'], 'a2_refused')
            self.assertEqual(site['transport'], 'http-series')
            sent.append(pending.pop(threading.get_ident()))
        self._a58_physical = physical
        self.addCleanup(delattr, self, '_a58_physical')
        with (patch.object(FetchLedger, 'decision', decision),
              patch.object(FetchLedger, 'result', result)):
            self.test_a55_default_success_all_six_direct_roots()
        self.assertEqual(pending, {})
        self.assertEqual(len(sent), 6)
        self.assertEqual(set(sent), results)
        self.assertEqual([item[0] for item in sent],
            ['http.stockanalysis.validation'] * 4 + ['http.stockanalysis.external_sweep'] * 2)
        original_line = site['line']
        site['line'] += 1
        fake_frame = type('Frame', (), {'f_code': type('Code', (), {
            'co_filename': str(Path(labels.__file__))}), 'f_lineno': original_line})()
        with self.assertRaisesRegex(AssertionError, 'physical source line must match inventory'):
            physical(fake_frame)
        print('A58_OWNED_PHYSICAL_BINDING six durable decision/open/result pairs; line inverse red')

    def test_sweep_disk_cache_replay_requires_sealed_portable_provenance(self):
        project = Path(tempfile.mkdtemp(dir=self.temp.name, prefix="sweep-replay-"))
        bank = project / "bank"
        logs = project / "Run Logs"
        cache = logs / "cache"
        days = []
        for month_index in range(12):
            year = 2025 + (8 + month_index - 1) // 12
            month = (8 + month_index - 1) % 12 + 1
            day = date(year, month, 3)
            while self.authority.window("1d", day) is None:
                day += timedelta(days=1)
            days.append(day.isoformat())
        snapshot = {
            "ticker": "TEST", "conid": 101, "provider_symbol": "TEST",
            "manifest_fingerprint": "a" * 64,
            "stored": {day: 100.0 for day in days},
            "stored_rows": len(days), "stored_first": min(days),
            "stored_last": max(days), "stored_basis": "raw",
            "basis_actions_applied": [],
        }
        raw = json.dumps({"data": [self.row(day, 100)
                                   for day in days]}).encode("utf-8")
        sent = []
        provider = external_sweep.StockAnalysisProvider(
            opener=lambda url, _timeout: sent.append(url) or raw,
            now=lambda: datetime(2026, 8, 4, tzinfo=timezone.utc))

        def run(root, evidence, captured="2026-08-04T17:00:00"):
            with (tp.offline_policy() as capability,
                  patch.object(external_sweep, "stored_daily_snapshot",
                               return_value=snapshot),
                  patch.object(external_sweep, "_manifest_unchanged",
                               return_value=True)):
                return external_sweep.sweep_ticker(
                    root / "bank", "TEST", provider=provider,
                    cache_root=root / "Run Logs" / "cache",
                    asof=datetime(2026, 8, 4, tzinfo=timezone.utc),
                    _test_capability=capability, _evidence_dir=evidence,
                    _authority=self.authority,
                    _clock=lambda: datetime.fromisoformat(captured).replace(
                        tzinfo=NY), _governors=self.governors)

        first = run(project, logs / "ledgers")
        self.assertEqual(first["reference_source"], "network")
        self.assertEqual(len(sent), 1)
        cache_path = external_sweep._cache_path(cache, snapshot)
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
        self.assertEqual(payload["http_attempt"]["operation_id"],
                         inspect_ledger(next((logs / "ledgers").glob(
                             "*.jsonl")))["operation_id"])
        self.assertEqual(len(payload["accepted_rows"]), len(days))
        self.assertFalse(Path(payload["ledger_locator"]).is_absolute())
        self.assertEqual(payload["attempt_id"],
                         inspect_ledger(next((logs / "ledgers").glob(
                             "*.jsonl")))["events"][0]["attempt_id"])
        good_bytes = cache_path.read_bytes()
        second = run(project, logs / "replay-ledgers")
        self.assertEqual(second["reference_source"], "cache")
        self.assertEqual(second["network_requests"], 0)
        self.assertEqual(len(sent), 1)
        self.assertEqual(cache_path.read_bytes(), good_bytes)

        moved = project.with_name(project.name + "-moved")
        shutil.copytree(project, moved)
        relocated = run(moved, moved / "Run Logs" / "relocated-ledgers")
        self.assertEqual(relocated["reference_source"], "cache")
        self.assertEqual(len(sent), 1)

        check_worker = external_sweep.fops.check_worker
        def cancelled_replay(context, worker_id):
            if inspect.currentframe().f_back.f_code.co_name == "_sweep_ticker_body":
                raise RequestCancelled("cancelled before cache replay")
            return check_worker(context, worker_id)
        with patch.object(external_sweep.fops, "check_worker", cancelled_replay):
            with self.assertRaisesRegex(RequestCancelled, "before cache replay"):
                run(project, logs / "cancelled-replay")
        permits = FetchRunContext.permits
        def refused_replay(context, producer, envelope):
            if producer == "http.stockanalysis.external_sweep":
                return False
            return permits(context, producer, envelope)
        with patch.object(FetchRunContext, "permits", refused_replay):
            with self.assertRaisesRegex(RequestRefused, "cache lacks operation rights"):
                run(project, logs / "no-rights-replay")
        self.assertEqual(len(sent), 1)
        self.assertEqual(cache_path.read_bytes(), good_bytes)
        for name in ("cancelled-replay", "no-rights-replay"):
            audit = inspect_ledger(next((logs / name).glob("*.jsonl")),
                                   require_seal=False)
            self.assertFalse(audit["verified"])
            self.assertEqual(audit["events"], [])

        mutations = {
            "wrong-attempt": lambda item: item.__setitem__(
                "attempt_id", "wrong-attempt"),
            "wrong-accepted-close": lambda item: item["accepted_rows"][0].__setitem__(
                "c", 999.0),
            "wrong-source-digest": lambda item: item.__setitem__(
                "source_bytes_sha256", "0" * 64),
            "missing-ledger": lambda item: item.__setitem__(
                "ledger_locator", "missing.jsonl"),
        }
        for name, mutate in mutations.items():
            with self.subTest(mutation=name):
                item = deepcopy(payload)
                mutate(item)
                cache_path.write_text(json.dumps(item), encoding="utf-8")
                before = len(sent)
                row = run(project, logs / ("mutant-" + name))
                self.assertEqual(row["reference_source"], "network")
                self.assertEqual(len(sent), before + 1)
        cache_path.write_bytes(good_bytes)
        before = len(sent)
        changed_horizon = run(project, logs / "new-horizon",
                              captured="2026-08-05T17:00:00")
        self.assertEqual(changed_horizon["reference_source"], "network")
        self.assertEqual(len(sent), before + 1)

        cache_path.write_bytes(good_bytes)
        source_ledger = logs / payload["ledger_locator"]
        receipt = source_ledger.with_suffix(".seal.json")
        receipt_bytes = receipt.read_bytes()
        receipt.write_bytes(b"{}")
        try:
            before = len(sent)
            unsealed = run(project, logs / "invalid-receipt")
            self.assertEqual(unsealed["reference_source"], "network")
            self.assertEqual(len(sent), before + 1)
        finally:
            receipt.write_bytes(receipt_bytes)

    def test_sweep_replay_admission_inverses_are_red(self):
        body = external_sweep._sweep_ticker_body
        for name, old, new, failure in (
                ("worker", "fops.check_worker(context, _fetch_worker.worker_id)",
                 "pass", "RequestCancelled not raised"),
                ("rights", 'if not context.permits("http.stockanalysis.external_sweep", envelope):',
                 'if False and not context.permits("http.stockanalysis.external_sweep", envelope):',
                 "RequestRefused not raised")):
            with self.subTest(inverse=name):
                mutant = _source_inverse(body, old, new)
                with patch.object(external_sweep, "_sweep_ticker_body", mutant):
                    with self.assertRaisesRegex(AssertionError, failure):
                        self.test_sweep_disk_cache_replay_requires_sealed_portable_provenance()
                print("A36_REPLAY_ADMISSION_INVERSE_RED", name)

    def test_spool_entry_bound_is_independently_witnessed(self):
        logs = Path(self.temp.name) / "spool-size"
        snapshot = {"manifest_fingerprint": "a" * 64}
        item = (logs / "cache", logs / "bank", {"ticker": "TEST"},
                {"padding": "x" * external_sweep.MAX_REFERENCE_BYTES}, snapshot)
        with external_sweep._CacheSpool(logs) as spool:
            with self.assertRaisesRegex(external_sweep.SpoolError, "spool bound"):
                spool.append(item)
            self.assertEqual(spool._entries, [])
            self.assertIsNone(spool._temporary)
            self.assertFalse(logs.exists())

    def test_spool_bound_cleanup_and_precommit_inverses_are_red(self):
        spool = external_sweep._CacheSpool
        bound_mutant = _source_inverse(spool.append,
            "len(encoded) > MAX_REFERENCE_BYTES", "False")
        with patch.object(spool, "append", bound_mutant):
            with self.assertRaisesRegex(AssertionError, "SpoolError not raised"):
                self.test_spool_entry_bound_is_independently_witnessed()
        print("A36_SPOOL_INVERSE_RED entry-bound")
        with patch.object(spool, "__exit__", lambda *_args: None):
            with self.assertRaisesRegex(AssertionError, "False is not true"):
                self.test_sweep_batch_spools_multiple_candidates_and_cleans_on_fault()
        print("A36_SPOOL_INVERSE_RED explicit-cleanup")
        writer = _source_inverse(external_sweep.write_cache,
            "storage._atomic_write_bytes(candidate, encoded)",
            "storage._atomic_write_bytes(candidate, encoded)\n"
            "        storage._atomic_write_bytes(path, encoded)")
        with patch.object(external_sweep, "write_cache", writer):
            with self.assertRaisesRegex(AssertionError, "precommit verification"):
                self.test_sweep_batch_spools_multiple_candidates_and_cleans_on_fault()
        print("A36_CACHE_INVERSE_RED write-before-verify")

    def test_sweep_batch_spools_multiple_candidates_and_cleans_on_fault(self):
        project = Path(tempfile.mkdtemp(dir=self.temp.name, prefix="sweep-spool-"))
        bank = project / "bank"
        logs = project / "Run Logs"
        cache = logs / "cache"
        days = []
        for month_index in range(12):
            year = 2025 + (8 + month_index - 1) // 12
            month = (8 + month_index - 1) % 12 + 1
            day = date(year, month, 3)
            while self.authority.window("1d", day) is None:
                day += timedelta(days=1)
            days.append(day.isoformat())
        raw = json.dumps({"data": [self.row(day, 100)
                                   for day in days]}).encode("utf-8")
        names = ("AAA", "BBB", "CCC")
        snapshots = {
            name: {"ticker": name, "conid": index,
                   "provider_symbol": name,
                   "manifest_fingerprint": str(index) * 64,
                   "stored": {day: 100.0 for day in days},
                   "stored_rows": len(days), "stored_first": min(days),
                   "stored_last": max(days), "stored_basis": "raw",
                   "basis_actions_applied": []}
            for index, name in enumerate(names, 1)
        }
        sent, spooled, spool_dirs, live_spools = [], [], [], []
        provider = external_sweep.StockAnalysisProvider(
            opener=lambda url, _timeout: sent.append(url) or raw,
            now=lambda: datetime(2026, 8, 4, tzinfo=timezone.utc))
        original_spool = external_sweep._CacheSpool

        class ObservedSpool(original_spool):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                # Keep the TemporaryDirectory alive through the cleanup check;
                # its finalizer must not make a missing __exit__ look green.
                live_spools.append(self)
                # A deliberately removed __exit__ must still be cleaned after
                # its negative assertion, without relying on a GC finalizer.
                self_outer.addCleanup(lambda: self._temporary.cleanup()
                                      if self._temporary is not None else None)

            def append(self, item):
                super().append(item)
                self_outer.assertEqual(len(self._entries), len(spooled) + 1)
                self_outer.assertTrue(all(isinstance(entry[-1], Path)
                                          and entry[-1].is_file()
                                          for entry in self._entries))
                self_outer.assertLessEqual(
                    self._total_bytes,
                    external_sweep.MAX_TICKERS *
                    external_sweep.MAX_REFERENCE_BYTES)
                spooled.append(item[2]["ticker"])
                spool_dirs.append(Path(self._temporary.name))

        self_outer = self

        def run(owner, evidence):
            with (tp.offline_policy() as capability,
                  patch.object(external_sweep, "stored_daily_snapshot",
                               side_effect=lambda _root, ticker:
                               snapshots[ticker]),
                  patch.object(external_sweep, "_manifest_unchanged",
                               return_value=True),
                  patch.object(external_sweep, "_manifest_record",
                               side_effect=lambda _root, ticker:
                               snapshots[ticker])):
                return external_sweep.sweep_bank(
                    bank, names, provider=provider, cache_root=cache,
                    run_logs_root=logs, owner=owner,
                    gate_path=logs / ("." + owner + ".lock"),
                    now=datetime(2026, 8, 4, tzinfo=timezone.utc),
                    clock=self.clock.now, sleep_fn=self.clock.sleep,
                    _test_capability=capability, _evidence_dir=evidence,
                    _authority=self.authority,
                    _clock=lambda: datetime(2026, 8, 4, 17, tzinfo=NY),
                    _governors=self.governors)

        with patch.object(external_sweep, "_CacheSpool", ObservedSpool):
            report = run("spoolok", logs / "success-ledgers")
        self.assertEqual(spooled, list(names))
        self.assertEqual(len(sent), 3)
        self.assertEqual(report["network_requests"], 3)
        self.assertTrue(Path(report["artifact"]).is_file())
        self.assertEqual(len(list(cache.glob("*.json"))), 3)
        self.assertTrue(all(not directory.exists() for directory in spool_dirs))
        before = {path.name: path.read_bytes() for path in cache.glob("*.json")}

        class FailingSpool(ObservedSpool):
            def append(self, item):
                if len(self._entries) == 1:
                    raise external_sweep.SpoolError("injected spool failure")
                super().append(item)

        spooled.clear()
        sent.clear()
        with (patch.object(external_sweep, "_CacheSpool", FailingSpool),
              patch.object(external_sweep, "load_cache",
                           return_value={"status": "missing", "usable": False,
                                         "path": "unused"})):
            with self.assertRaisesRegex(external_sweep.SpoolError,
                                        "injected spool failure"):
                run("spoolfail", logs / "failure-ledgers")
        self.assertEqual(spooled, ["AAA"])
        self.assertEqual(len(sent), 2)
        self.assertEqual(before, {path.name: path.read_bytes()
                                  for path in cache.glob("*.json")})
        self.assertFalse((logs / "external-sweep-20260804-spoolfail.json").exists())
        self.assertTrue(all(not directory.exists() for directory in spool_dirs))
        self.assertFalse(inspect_ledger(next((logs / "failure-ledgers").glob(
            "*.jsonl")), require_seal=False)["verified"])

        real_write_cache = external_sweep.write_cache

        def fail_second_cache(target, root, identity, payload):
            if identity["ticker"] == "BBB":
                raise OSError("injected cache publication failure")
            return real_write_cache(target, root, identity, payload)

        sent.clear()
        with (patch.object(external_sweep, "load_cache",
                           return_value={"status": "missing", "usable": False,
                                         "path": "unused"}),
              patch.object(external_sweep, "write_cache",
                           side_effect=fail_second_cache)):
            with self.assertRaises(
                    external_sweep.PublicationRecoveryDebt) as caught:
                run("cachedebt", logs / "cache-debt-ledgers")
        self.assertEqual(len(sent), 3)
        self.assertEqual(caught.exception.items, [
            {"ticker": "BBB", "stage": "cache", "cause_type": "OSError"},
            {"ticker": "CCC", "stage": "cache_not_attempted",
             "cause_type": None},
        ])
        self.assertTrue(Path(caught.exception.report["artifact"]).is_file())
        debt_artifact = json.loads(Path(
            caught.exception.report["artifact"]).read_text(encoding="utf-8"))
        self.assertEqual(debt_artifact["publication_state"], "recovery_debt")
        self.assertEqual(debt_artifact["recovery_debt"],
                         caught.exception.items)
        after_debt = {path.name: path.read_bytes()
                      for path in cache.glob("*.json")}
        self.assertNotEqual(after_debt["AAA__1.json"], before["AAA__1.json"])
        self.assertEqual(after_debt["BBB__2.json"], before["BBB__2.json"])
        self.assertEqual(after_debt["CCC__3.json"], before["CCC__3.json"])
        self.assertFalse(list(logs.glob("external-sweep-spool-*")))

        sent.clear()
        with (patch.object(external_sweep, "load_cache",
                           return_value={"status": "missing", "usable": False,
                                         "path": "unused"}),
              patch.object(external_sweep, "write_artifact",
                           side_effect=OSError("injected report failure"))):
            with self.assertRaises(
                    external_sweep.PublicationRecoveryDebt) as report_caught:
                run("reportfail", logs / "report-debt-ledgers")
        self.assertEqual(len(sent), 3)
        self.assertEqual(report_caught.exception.items, [
            {"ticker": None, "stage": "report", "cause_type": "OSError"},
        ])
        self.assertEqual(report_caught.exception.report["publication_state"],
                         "recovery_debt")
        self.assertFalse((logs / "external-sweep-20260804-reportfail.json").exists())
        self.assertFalse(list(logs.glob("external-sweep-spool-*")))

        old_aaa = next(cache.glob("AAA__*.json"))
        old_bytes = old_aaa.read_bytes()
        candidate_payload = json.loads(old_bytes)
        candidate_payload["fetched_at"] = "2026-08-05T00:00:00+00:00"
        candidate_bytes = (json.dumps(candidate_payload, sort_keys=True,
                                      separators=(",", ":")) + "\n").encode()
        self.assertNotEqual(candidate_bytes, old_bytes)
        validate = external_sweep.validate_cache_payload
        validated = []

        def fail_after_staging(payload, identity):
            validated.append(True)
            if len(validated) == 2:
                raise external_sweep.EvidenceError(
                    "injected candidate verification failure")
            return validate(payload, identity)

        with patch.object(external_sweep, "validate_cache_payload",
                          side_effect=fail_after_staging):
            with self.assertRaisesRegex(
                    external_sweep.EvidenceError,
                    "injected candidate verification failure"):
                external_sweep.write_cache(
                    cache, bank, snapshots["AAA"], candidate_payload)
        self.assertEqual(len(validated), 2)
        self.assertEqual(old_aaa.read_bytes(), old_bytes,
                         "precommit verification must preserve last-good bytes")
        self.assertFalse(list(cache.glob("external-sweep-candidate-*")))

        with (tp.offline_policy() as capability,
              patch.object(external_sweep, "stored_daily_snapshot",
                           return_value=snapshots["AAA"]),
              patch.object(external_sweep, "_manifest_unchanged",
                           return_value=True),
              patch.object(external_sweep, "load_cache",
                           return_value={"status": "missing", "usable": False,
                                         "path": "unused"}),
              patch.object(external_sweep, "write_cache",
                           side_effect=OSError("injected single cache fault"))):
            with self.assertRaises(
                    external_sweep.PublicationRecoveryDebt) as single_caught:
                external_sweep.sweep_ticker(
                    bank, "AAA", provider=provider, cache_root=cache,
                    _test_capability=capability,
                    _evidence_dir=logs / "single-debt-ledgers",
                    _authority=self.authority,
                    _clock=lambda: datetime(2026, 8, 4, 17, tzinfo=NY),
                    _governors=self.governors)
        self.assertEqual(single_caught.exception.items, [
            {"ticker": "AAA", "stage": "cache", "cause_type": "OSError"},
        ])
        self.assertEqual(old_aaa.read_bytes(), old_bytes)
        self.assertTrue(inspect_ledger(next((logs / "single-debt-ledgers").glob(
            "*.jsonl")))["verified"])

    def test_sweep_cli_emits_structured_post_seal_recovery_debt(self):
        debt = [{"ticker": "TEST", "stage": "cache",
                 "cause_type": "OSError"}]
        error = external_sweep.PublicationRecoveryDebt(
            debt, {"artifact": "portable-test-report.json"})
        output = io.StringIO()
        with (patch.object(external_sweep, "sweep_bank", side_effect=error),
              redirect_stdout(output)):
            status = external_sweep_cli.main(
                ["sweep", "--allow-network", "--ticker", "TEST"],
                provider=object(), root=Path(self.temp.name) / "bank",
                cache_root=Path(self.temp.name) / "cache",
                run_logs_root=Path(self.temp.name) / "Run Logs")
        response = json.loads(output.getvalue())
        self.assertEqual(status, 2)
        self.assertEqual(response["error"]["type"],
                         "PublicationRecoveryDebt")
        self.assertEqual(response["recovery_debt"], debt)
        self.assertEqual(response["artifact"], "portable-test-report.json")

    def test_bridge_rejects_duplicate_wire_fields_and_quarantines(self):
        raw = (b'{"data":[{"t":"2026-08-03","t":"2026-08-04",'
               b'"o":10,"h":11,"l":9,"c":10,"v":100}]}')
        with tp.offline_policy() as capability:
            context = self.context(capability=capability)
            with self.assertRaises(ResponseQuarantined):
                guarded_stockanalysis_attempt(
                    context.worker("stockanalysis"),
                    "http.stockanalysis.validation", "TEST", "Max",
                    lambda url: raw)
            context.seal()
        self.assertEqual(inspect_ledger(context.ledger.path)["events"][1]
                         ["payload"]["outcome"], "error")
        self.assertEqual(len(list(context.ledger.path.parent.glob(
            context.ledger.path.stem + ".*.quarantine.json"))), 1)

    def test_range_less_pre_settle_refuses_before_transport(self):
        with tp.offline_policy() as capability:
            context = self.context("2026-08-04T13:20:00", capability)
            bounds = stockanalysis_bounds(
                context.authority, context.captured_now, "Max")
            request = context.worker("stockanalysis").request(
                "http.stockanalysis.validation", {
                    "variant": "http-series", "endpoint": "stockanalysis.history",
                    "subject": "TEST", "token": "1d",
                    "intended_start": bounds["intended_start"],
                    "intended_end": bounds["intended_end"]})
            sent = []
            with self.assertRaises(RequestRefused):
                request.execute(lambda effective: sent.append(1) or b"wire",
                    acquire_turn=lambda: 0,
                    governor=self.governors.provider("stockanalysis.history"),
                    normalizer=lambda response: [],
                    attempt_evidence={
                        "url": stockanalysis_url("TEST", "Max"), "range": "Max",
                        "requested_start_date": bounds["requested_start_date"],
                        "truncated_coverage": bounds["truncated_coverage"],
                    })
            context.seal()
        self.assertEqual(sent, [])
        events = inspect_ledger(context.ledger.path)["events"]
        self.assertEqual(events[0]["payload"]["reason"],
                         "range-less HTTP requires daily settlement")

    def test_coverage_clamp_provenance_is_durable_and_mutant_red(self):
        raw = json.dumps({"data": [self.row("1982-08-03")]}).encode("utf-8")
        with tp.offline_policy() as capability:
            context = self.context("1982-08-04T17:00:00", capability)
            bounds = stockanalysis_bounds(
                context.authority, context.captured_now, "5Y")
            self.assertTrue(bounds["truncated_coverage"])
            guarded_stockanalysis_attempt(
                context.worker("stockanalysis"),
                "http.stockanalysis.validation", "TEST", "5Y",
                lambda _url: raw)
            context.seal()
        decision = inspect_ledger(context.ledger.path)["events"][0]["payload"]
        self.assertEqual(decision["http_request"], {
            "url": stockanalysis_url("TEST", "5Y"), "range": "5Y",
            "requested_start_date": "1977-08-04",
            "truncated_coverage": True,
        })
        original = labels.verified_stockanalysis_attempt_evidence
        with tp.offline_policy() as capability:
            mutant_context = self.context("1982-08-04T17:00:00", capability)
            with patch.object(labels, "verified_stockanalysis_attempt_evidence",
                              side_effect=lambda *args: {
                                  **original(*args), "truncated_coverage": False}):
                guarded_stockanalysis_attempt(
                    mutant_context.worker("stockanalysis"),
                    "http.stockanalysis.validation", "TEST", "5Y",
                    lambda _url: raw)
            mutant_context.seal()
        mutant = inspect_ledger(mutant_context.ledger.path)["events"][0]
        with self.assertRaises(AssertionError):
            self.assertTrue(mutant["payload"]["http_request"]["truncated_coverage"])
        print("A23A_INVERSE_RED missing-five-year-truncation-evidence")

    def test_missing_or_tampered_provenance_refuses_before_transport(self):
        with tp.offline_policy() as capability:
            context = self.context(capability=capability)
            bounds = stockanalysis_bounds(
                context.authority, context.captured_now, "5Y")
            correct = {
                "url": stockanalysis_url("TEST", "5Y"), "range": "5Y",
                "requested_start_date": bounds["requested_start_date"],
                "truncated_coverage": bounds["truncated_coverage"],
            }
            sent = []
            for evidence in (None, {**correct, "range": "Max"},
                             {**correct, "truncated_coverage": True},
                             {**correct, "url": "https://example.invalid/"}):
                with self.subTest(evidence=evidence):
                    request = context.worker("stockanalysis").request(
                        "http.stockanalysis.validation", {
                            "variant": "http-series",
                            "endpoint": "stockanalysis.history",
                            "subject": "TEST", "token": "1d",
                            "intended_start": bounds["intended_start"],
                            "intended_end": bounds["intended_end"],
                        })
                    with self.assertRaises(RequestRefused):
                        request.execute(
                            lambda _effective: sent.append(1) or b"wire",
                            acquire_turn=lambda: 0,
                            governor=self.governors.provider(
                                "stockanalysis.history"),
                            normalizer=lambda response: [],
                            attempt_evidence=evidence)
            context.seal()
        self.assertEqual(sent, [])
        self.assertEqual(len(inspect_ledger(context.ledger.path)["events"]), 1)

    def test_closed_future_and_unsupported_labels_stay_observed(self):
        with tp.offline_policy() as capability:
            context = self.context(capability=capability)
            before_coverage = (self.authority.first_date - timedelta(days=1)).isoformat()
            accepted, _ = self.execute(context, [
                self.row(before_coverage), self.row("2026-08-01"),
                self.row("2026-08-03"), self.row("2026-08-05")], rng="Max")
            context.seal()
        self.assertEqual([row["label"] for row in accepted], ["2026-08-03"])
        result = inspect_ledger(context.ledger.path)["events"][1]["payload"]
        self.assertEqual((result["observed"]["count"],
                          result["accepted"]["count"], result["dropped"]),
                         (4, 1, 3))
        self.assertNotEqual(result["observed"]["digest"],
                            result["accepted"]["digest"])

    def test_malformed_and_duplicate_dates_fail_and_quarantine(self):
        for rows in ([self.row("nonsense")],
                     [self.row("2026-08-03 nonsense")],
                     [self.row("2026-08-03"), self.row("2026-08-03")]):
            with self.subTest(rows=rows), tp.offline_policy() as capability:
                context = self.context(capability=capability)
                with self.assertRaises(ResponseQuarantined):
                    self.execute(context, rows)
                context.seal()
                result = inspect_ledger(context.ledger.path)["events"][1]["payload"]
                self.assertEqual(result["outcome"], "error")
                self.assertEqual(len(list(context.ledger.path.parent.glob(
                    context.ledger.path.stem + ".*.quarantine.json"))), 1)

    def test_epoch_label_is_not_shifted_to_new_york_day(self):
        epoch = int(datetime(2026, 8, 4, 0, 30, tzinfo=timezone.utc).timestamp())
        with tp.offline_policy() as capability:
            context = self.context(capability=capability)
            rows, _ = self.execute(context, [self.row(epoch)])
            context.seal()
        self.assertEqual(rows[0]["label"], "2026-08-04")

    def test_label_and_range_in_memory_mutants_are_red(self):
        epoch = int(datetime(2026, 8, 4, 0, 30, tzinfo=timezone.utc).timestamp())
        with patch.object(stock_validate, "_norm_date_key",
                          return_value="2026-08-03"):
            with self.assertRaises(AssertionError):
                self.assertEqual(labels.normalize_stockanalysis_rows(
                    [self.row(epoch)])[0]["label"], "2026-08-04")
        print("A23A_INVERSE_RED shifted-utc-label")

        with tp.offline_policy() as capability:
            context = self.context(capability=capability)
            with patch.object(labels, "filter_stockanalysis_rows",
                              side_effect=lambda rows, *_: [
                                  {**row, "timestamp": "2026-08-03T20:00:00+00:00"}
                                  for row in rows]):
                accepted, _ = self.execute(context, [
                    self.row("2026-08-03"), self.row("2026-08-01")], rng="Max")
                with self.assertRaises(AssertionError):
                    self.assertEqual([row["label"] for row in accepted],
                                     ["2026-08-03"])
            context.seal()
        print("A23A_INVERSE_RED closed-label-accepted")

        context = self.context()
        correct = stockanalysis_bounds(context.authority,
                                       context.captured_now, "5Y")
        rolling = (context.captured_now.date() - timedelta(days=5 * 365))
        with self.assertRaises(AssertionError):
            self.assertEqual(rolling.isoformat(), correct["requested_start_date"])
        print("A23A_INVERSE_RED rolling-365-five-year-range")


if __name__ == "__main__":
    result = unittest.TextTestRunner(verbosity=2).run(
        unittest.defaultTestLoader.loadTestsFromTestCase(HttpLabels))
    print(f"{result.testsRun} checks, {len(result.errors) + len(result.failures)} failed")
    raise SystemExit(not result.wasSuccessful())
