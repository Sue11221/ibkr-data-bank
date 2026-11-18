"""Offline A2-3 HTTP label/range acceptance through the shipped executor."""

import ast
import base64
from datetime import date, datetime, time, timedelta, timezone
from dataclasses import FrozenInstanceError, replace
from contextlib import redirect_stdout, nullcontext, ExitStack
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
import urllib.response
import ssl
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import Mock, patch
from email.message import Message
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, urlsplit

import fetch_test_policy as tp

# Transport, process and GUI tripwires precede every production import.
with tp.offline_transports():
    from fetch_authority import (AuthorityError, NY, ResponseQuarantined,
                                 ScheduleAuthority, digest_value)
    from fetch_governors import ENDPOINTS, GovernorRegistry
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
    import fetch_http_splits as splits
    import fetch_ledger as ledger_module


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

_A54_TARGETS = (
    ("stockanalysis.history", "stockanalysis.com"),
    ("yahoo.splits", "query1.finance.yahoo.com"),
    ("sec.tickers", "www.sec.gov"),
    ("sec.companyfacts", "data.sec.gov"),
    ("fortinet.history", "www.fortinet.com"),
    ("sp500.github", "raw.githubusercontent.com"),
    ("sp500.wikipedia", "en.wikipedia.org"),
)

# Independent expansion IDs, current owned physical site and historical seam
# lineage. Shared opens are NOT additional logical or endpoint identities.
_A64_HTTP_BINDINGS = (
    ('http.stockanalysis.validation', 'stockanalysis.history', 'http-series',
     'engine.fetch_http_labels:guarded_stockanalysis_attempt.one_send:http_opener_open:1',
     'stock_validate.fetch_daily_reference', 'validation'),
    ('http.stockanalysis.external_sweep', 'stockanalysis.history', 'http-series',
     'engine.fetch_http_labels:guarded_stockanalysis_attempt.one_send:http_opener_open:1',
     'external_sweep.StockAnalysisProvider._read', 'external_sweep'),
    ('http.split_provider.yahoo', 'yahoo.splits', 'http-series',
     'engine.fetch_http_splits:guarded_split_attempt.one_send:http_opener_open:1',
     'split_provider._default_fetcher', 'split_refresh'),
    ('http.split_provider.sec_tickers', 'sec.tickers', 'http-metadata',
     'engine.fetch_http_splits:guarded_split_attempt.one_send:http_opener_open:1',
     'split_provider._default_fetcher', 'split_refresh'),
    ('http.split_provider.sec_facts', 'sec.companyfacts', 'http-metadata',
     'engine.fetch_http_splits:guarded_split_attempt.one_send:http_opener_open:1',
     'split_provider._default_fetcher', 'split_refresh'),
    ('http.split_provider.issuer', 'fortinet.history', 'http-metadata',
     'engine.fetch_http_splits:guarded_split_attempt.one_send:http_opener_open:1',
     'split_provider._default_fetcher', 'split_refresh'),
    ('http.sp500.github', 'sp500.github', 'http-metadata',
     'engine.fetch_http_membership:guarded_membership_attempt.one_send:http_opener_open:1',
     'sp500._http', 'membership'),
    ('http.sp500.wikipedia', 'sp500.wikipedia', 'http-metadata',
     'engine.fetch_http_membership:guarded_membership_attempt.one_send:http_opener_open:1',
     'sp500._http', 'membership'),
)


# The forty unique calls behind forty static reverse records and nine overlapping
# references. This is a source/forwarding denominator, NOT forty HTTP positives.
_A67_STATIC_EDGES = (
    # Public copy: the five archived one-off script edges are left out with the archive.
    ('display_data.py', 'DataViewerApp._storage_xval_revalidate._work_body', 3824, 'stock_validate.cross_validate_ticker', 3, (), 'validation-gui'),
    ('display_data.py', 'DataViewerApp._xval_worker', 4077, 'stock_validate.cross_validate_ticker', 3, (), 'validation-gui'),
    ('display_data.py', 'DataViewerApp._storage_verify_dialog.run.work', 6488, 'stock_validate.validate_series', 3, ('sensitivity', 'save_reference'), 'validation-gui'),
    ('display_data.py', 'DataViewerApp._storage_verify_dialog.run_health.work', 6512, 'export_quality.ensure_current_health', 1, ('current_fn',), 'export-health'),
    ('display_data.py', 'DataViewerApp._storage_verify_dialog.run_split_refresh.work', 6537, 'split_cache.refresh_ticker', 2, ('progress', 'cancel'), 'split-entry'),
    ('display_data.py', 'DataViewerApp._storage_verify_dialog.run_split_refresh.work', 6546, 'export_quality.ensure_current_health', 1, ('current_fn',), 'export-health'),
    ('display_data.py', 'DataViewerApp._storage_export_run._work', 10131, 'export_quality.ensure_current_health', 1, (), 'export-health'),
    ('display_data.py', 'DataViewerApp._storage_export_run._work', 10137, 'export_batch.run_storage_batch', 3, ('interval', 'mode', 'start_date', 'end_date', 'preset', 'hours_mode', 'file_format', 'progress'), 'export-health'),
    ('display_data.py', 'DataViewerApp._exd_export._work', 11716, 'export_quality.ensure_current_health', 1, (), 'export-health'),
    ('display_data.py', 'DataViewerApp._exd_export._work', 11722, 'export_batch.run_designer_batch', 5, ('progress', 'cancel'), 'export-health'),
    ('display_data.py', 'DataViewerApp._addstock_consume_portfree_debt._work', 12423, 'stock_validate.cross_validate_ticker', 3, (), 'validation-gui'),
    ('display_data.py', 'DataViewerApp._addstock_consume_portfree_debt._runner', 12525, '_work', 0, (), 'validation-gui'),
    ('display_data.py', 'DataViewerApp._storage_find_start_fill._probe_check', 14395, 'stock_validate.cross_validate_ticker', 3, (), 'validation-gui'),
    ('engine/deep_seam_scan.py', '<module>', 300, 'scan_ticker', 2, (), 'cli-entry'),
    ('engine/export_batch.py', '_bundle_current_health', 142, 'export_quality.ensure_current_health', 1, (), 'export-health'),
    ('engine/export_batch.py', 'run_batch', 518, '_bundle_current_health', 2, (), 'export-health'),
    ('engine/export_batch.py', 'run_storage_batch', 895, 'run_batch', 4, ('interval', 'file_format', 'sessions', 'start', 'end', 'filename_for', 'exporter', 'workers', 'progress', 'cancel', 'now', 'quality_kwargs', 'planned_months_for'), 'export-health'),
    ('engine/export_batch.py', 'run_designer_batch', 963, 'run_batch', 4, ('interval', 'file_format', 'sessions', 'start', 'end', 'filename_for', 'exporter', 'direct_target', 'workers', 'progress', 'cancel', 'now', 'quality_kwargs', 'planned_months_for'), 'export-health'),
    ('engine/export_quality.py', 'ensure_current_health', 263, 'health_report.audit', 1, ('write',), 'export-health'),
    ('engine/external_sweep_cli.py', '<module>', 112, 'main', 0, (), 'cli-entry'),
    ('engine/external_sweep_cli.py', 'main', 82, 'sweep.sweep_bank', 2, ('provider', 'cache_root', 'run_logs_root', 'owner', 'pace', 'attempts', 'backoff', 'gate_path', 'now', None), 'cli-entry'),
    ('engine/health.py', '<module>', 796, 'main', 0, (), 'cli-entry'),
    ('engine/health.py', 'main', 789, 'audit', 1, ('project_root', 'write'), 'cli-entry'),
    ('engine/live_catchup.py', '<module>', 71, 'main', 0, (), 'cli-entry'),
    ('engine/live_catchup.py', 'main', 33, 'sv.cross_validate_ticker', 2, ('interval', 'rng'), 'cli-entry'),
    ('engine/live_internal_daily_check.py', '<module>', 299, 'main', 0, (), 'cli-entry'),
    ('engine/live_internal_daily_check.py', 'main', 292, 'run_internal_daily_check', 0, ('port', 'ticker', 'days'), 'cli-entry'),
    ('engine/phantom_fix.py', '<module>', 301, 'main', 0, (), 'cli-entry'),
    ('engine/phantom_fix.py', 'main', 283, 'apply_correction', 1, ('ticker', 'ex_date', 'factor', 'intervals', 'snapshot_root', 'dry_run'), 'cli-entry'),
    ('engine/split_cache.py', '<module>', 1313, 'main', 0, (), 'split-entry'),
    ('engine/split_cache.py', 'main', 1266, 'run_refresh_cli', 2, ('refresh_bank', 'provider', 'provider_symbols', 'cik_bindings', 'pace_seconds', 'limit', 'reference_fetcher', 'scan_fn', 'timeout', 'artifact', '_test_capability', '_evidence_dir', '_authority', '_clock', '_governors', '_fault', '_sec_user_agent', '_budget_seconds'), 'split-entry'),
    ('engine/triage_classifier.py', '<module>', 698, 'main', 0, (), 'cli-entry'),
    ('engine/triage_classifier.py', 'main', 677, 'audit', 1, ('flags', 'write'), 'cli-entry'),
    ('engine/wbd_truncate.py', '<module>', 570, 'main', 0, (), 'cli-entry'),
    ('engine/wbd_truncate.py', 'main', 551, 'apply_truncation', 1, ('ticker', 'boundary', 'cutover', 'intervals', 'snapshot_root', 'dry_run'), 'cli-entry'),
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

    def a55_opener_scheme_witness(self, endpoint, scheme):
        host = dict(_A54_TARGETS)[endpoint]
        lookups, protocols = [], []
        def lookup(target):
            lookups.append(target.host)
            raise OSError('A55 guarded DNS sentinel')
        def truncated_connect(connection):
            # The real ctor/host guard precedes this finite no-native cutoff.
            return http_transport.resolve(connection._deadline)
        def protocol_tripwire(handler, request):
            protocols.append(request.type)
            raise URLError('A55 default protocol tripwire')
        url = ('data:text/plain,finite-fixture' if scheme == 'data' else
               'file:///A55-no-file-read' if scheme == 'file' else
               scheme + '://' + host + '/finite-fixture')
        with (patch.object(http_transport, '_lookup_addresses', lookup),
              patch.object(http_transport, '_DNS_SLOT', threading.BoundedSemaphore(1)),
              patch.object(http_transport, '_make_socket') as sockets,
              patch.object(http_transport.DeadlineHTTPSConnection, 'connect', truncated_connect),
              patch.object(urllib.request.HTTPHandler, 'http_open', protocol_tripwire),
              patch.object(urllib.request.FTPHandler, 'ftp_open', protocol_tripwire),
              patch.object(urllib.request.FileHandler, 'file_open', protocol_tripwire),
              patch.object(urllib.request.DataHandler, 'data_open', protocol_tripwire),
              http_transport.Deadline(20, endpoint=endpoint) as deadline):
            opener = http_transport.build_opener(deadline)
            wire = urllib.request.Request(url, method='GET')
            with self.assertRaises(URLError) as caught:
                urllib.request.OpenerDirector.open(opener, wire, timeout=deadline.remaining())
            sockets.assert_not_called()
        self.assertEqual(protocols, [], 'non-HTTPS protocol dispatch is forbidden')
        self.assertEqual(lookups, [host] if scheme == 'https' else [])
        if scheme == 'https':
            self.assertEqual(str(caught.exception.reason), 'A55 guarded DNS sentinel')

    def test_a55_https_only_opener_and_scheme_inverse(self):
        for endpoint, _host in _A54_TARGETS:
            for scheme in ('https', 'http', 'ftp', 'file', 'data', 'a55unknown'):
                with self.subTest(endpoint=endpoint, scheme=scheme):
                    self.a55_opener_scheme_witness(endpoint, scheme)
        mutant = _source_inverse(http_transport.build_opener,
            'opener = urllib.request.OpenerDirector()',
            'opener = urllib.request.build_opener()')
        with patch.object(http_transport, 'build_opener', mutant):
            for endpoint, _host in _A54_TARGETS:
                for scheme in ('http', 'ftp', 'file', 'data'):
                    with self.subTest(inverse_endpoint=endpoint, scheme=scheme):
                        with self.assertRaisesRegex(AssertionError, 'protocol dispatch is forbidden'):
                            self.a55_opener_scheme_witness(endpoint, scheme)
                        print('A55_SCHEME_INVERSE_RED', endpoint, scheme)
        # UnknownHandler is an explicit stdlib refusal backstop, not a claimed
        # independent inverse: defaults also reject unknown schemes.

    def a56_error_processing_witness(self, endpoint, code):
        host = dict(_A54_TARGETS)[endpoint]
        url = 'https://' + host + '/a56-finite-response'
        opened, reads = [], []

        class Body(io.BytesIO):
            def read(self, *args):
                reads.append('read')
                return super().read(*args)

            def read1(self, *args):
                reads.append('read1')
                return super().read1(*args)

        body = Body(b'provider error body must remain unread')
        headers = Message()
        headers['Location'] = 'https://not-the-admitted-host.example/redirect'
        response = urllib.response.addinfourl(body, headers, url, code)
        response.msg = 'A56 finite response'

        def one_wire(handler, wire):
            opened.append(wire.full_url)
            return response

        try:
            with (patch.object(http_transport.DeadlineHTTPSHandler, 'https_open', one_wire),
                  patch.object(http_transport, '_lookup_addresses') as lookup,
                  patch.object(http_transport, '_make_socket') as sockets,
                  http_transport.Deadline(20, endpoint=endpoint) as deadline):
                opener = http_transport.build_opener(deadline)
                wire = urllib.request.Request(url, method='GET')
                with self.assertRaises(HTTPError) as caught:
                    urllib.request.OpenerDirector.open(opener, wire, timeout=deadline.remaining())
                self.assertEqual(caught.exception.code, code, 'error status must remain exact')
                self.assertEqual(caught.exception.url, url)
                self.assertIs(caught.exception.fp, response)
                lookup.assert_not_called()
                sockets.assert_not_called()
            self.assertEqual(opened, [url], 'one wire open; redirects must not follow')
            self.assertEqual(reads, [], 'error and redirect bodies must remain unread')
        finally:
            response.close()
        self.assertTrue(body.closed)

    def test_a56_real_director_error_handlers_and_removal_inverses(self):
        for endpoint, _host in _A54_TARGETS:
            for code in (301, 302, 303, 307, 308, 304, 404, 429, 500):
                with self.subTest(endpoint=endpoint, code=code):
                    self.a56_error_processing_witness(endpoint, code)
        for handler in ('HTTPErrorProcessor', 'HTTPDefaultErrorHandler'):
            mutant = _source_inverse(http_transport.build_opener,
                'urllib.request.' + handler + '(),', '')
            with patch.object(http_transport, 'build_opener', mutant):
                for endpoint, _host in _A54_TARGETS:
                    for code in (404, 429, 500):
                        with self.subTest(removed=handler, endpoint=endpoint, code=code):
                            with self.assertRaisesRegex(AssertionError, 'HTTPError not raised'):
                                self.a56_error_processing_witness(endpoint, code)
                            print('A56_ERROR_HANDLER_INVERSE_RED', handler, endpoint, code)

    def a55_stockanalysis_caller_witness(self):
        lookups = []
        def lookup(target):
            lookups.append(target.host)
            raise OSError('A55 caller guarded DNS sentinel')
        def truncated_connect(connection):
            return http_transport.resolve(connection._deadline)
        with tp.offline_policy() as capability:
            context = self.context(capability=capability)
            with (patch.object(http_transport, '_lookup_addresses', lookup),
                  patch.object(http_transport, '_DNS_SLOT', threading.BoundedSemaphore(1)),
                  patch.object(http_transport, '_make_socket') as sockets,
                  patch.object(http_transport.DeadlineHTTPSConnection, 'connect', truncated_connect)):
                with self.assertRaises((URLError, AuthorityError)) as caught:
                    labels.guarded_stockanalysis_attempt(context.worker('a55-real-caller'),
                        'http.stockanalysis.validation', 'TEST', 'Max')
                sockets.assert_not_called()
            context.seal()
        self.assertEqual(lookups, ['stockanalysis.com'], 'caller target must reach its own guarded host')
        self.assertIsInstance(caught.exception, URLError)
        self.assertEqual(str(caught.exception.reason), 'A55 caller guarded DNS sentinel')
        events = inspect_ledger(context.ledger.path)['events']
        self.assertEqual(len(events), 3, 'one durable decision/result and seal')
        self.assertEqual(events[1]['payload']['outcome'], 'error')

    def test_a55_stockanalysis_caller_target_and_inverse(self):
        self.a55_stockanalysis_caller_witness()
        mutant = _source_inverse(labels.guarded_stockanalysis_attempt,
            'with Deadline(timeout) as deadline:',
            'with Deadline(timeout, endpoint="yahoo.splits") as deadline:')
        with patch.object(labels, 'guarded_stockanalysis_attempt', mutant):
            with self.assertRaisesRegex(AssertionError, 'caller target must reach its own guarded host'):
                self.a55_stockanalysis_caller_witness()
        print('A55_CALLER_TARGET_INVERSE_RED stockanalysis.history')

    def a55_five_year_lower_bound_witness(self):
        with tp.offline_policy() as capability:
            context = self.context(capability=capability)
            accepted, raw = self.execute(context, [self.row('2021-08-03'), self.row('2021-08-04')])
            context.seal()
        self.assertEqual([row['label'] for row in accepted], ['2021-08-04'],
                         '5Y pre-start row must be observed but dropped')
        result = inspect_ledger(context.ledger.path)['events'][1]['payload']
        self.assertEqual((result['observed']['count'], result['accepted']['count'], result['dropped']), (2, 1, 1))
        self.assertEqual(result['raw_response_digest'], hashlib.sha256(raw).hexdigest())

    def test_a52_stockanalysis_5y_lower_bound(self):
        self.a55_five_year_lower_bound_witness()
        mutant = _source_inverse(labels.filter_stockanalysis_rows,
            'or day < envelope.intended_start.date()', '')
        with patch.object(labels, 'filter_stockanalysis_rows', mutant):
            with self.assertRaisesRegex(AssertionError, '5Y pre-start row'):
                self.a55_five_year_lower_bound_witness()
        print('A55_FIVE_YEAR_LOWER_BOUND_INVERSE_RED')

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
        def lookup(target):
            self.assertEqual(target.host, 'stockanalysis.com')
            calls.append('lookup')
            entered.set()
            self.assertTrue(release.wait(2), 'test must release its DNS worker')
            return [(2, 1, 6, '', ('127.0.0.1', 443))]
        class TinyDeadline:
            target = http_transport.RequestTarget('stockanalysis.history')
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

    def a54_dns_busy_witness(self, endpoint):
        slot, launched = threading.BoundedSemaphore(1), []
        self.assertTrue(slot.acquire(blocking=False))
        deadline = Mock(target=http_transport.RequestTarget(endpoint))
        deadline.remaining.return_value = 20
        class Thread:
            def __init__(_self, *, target, args, **kwargs):
                _self.run = lambda: target(*args)
            def start(_self):
                launched.append(endpoint)
                _self.run()
        try:
            with (patch.object(http_transport, '_DNS_SLOT', slot),
                  patch.object(http_transport.threading, 'Thread', Thread),
                  patch.object(http_transport, '_lookup_addresses', return_value=[
                      (2, 1, 6, '', ('127.0.0.1', 443))]),
                  patch.object(http_transport, '_make_socket') as opened):
                with self.assertRaisesRegex(AuthorityError, 'still occupied'):
                    http_transport.connect_resolved(deadline, (deadline.target.host, 443), 20)
                self.assertEqual(launched, [], 'busy DNS must not start a resolver')
                opened.assert_not_called()
        finally:
            # The deliberate guard-removal mutant may have released the held
            # fixture slot. Acquire it if free, then balance the one held token.
            slot.acquire(blocking=False)
            slot.release()

    def test_a52_split_dns_cross_host_busy_and_late(self):
        # Keep the original mechanics and add actual root/ledger/publication
        # evidence below. Full four-way root inverses are tracked separately.
        slot = threading.BoundedSemaphore(1)
        entered, release = threading.Event(), threading.Event()
        lookups, connected, closed = [], [], []
        def lookup(target):
            lookups.append(target.host)
            if target.endpoint_id == 'yahoo.splits':
                entered.set()
                if not release.wait(3):
                    raise AssertionError('owned fixture resolver was not released')
            return [(2, 1, 6, '', ('127.0.0.1', 443))]
        class Socket:
            def settimeout(_self, timeout):
                self.assertGreater(timeout, 0)
            def connect(_self, address):
                connected.append(address)
            def close(_self):
                closed.append(True)
        sock = Socket()
        with (patch.object(http_transport, '_DNS_SLOT', slot),
              patch.object(http_transport, '_lookup_addresses', lookup),
              patch.object(http_transport, '_make_socket', return_value=sock) as opened,
              patch.object(urllib.request.OpenerDirector, 'open') as http_open,
              patch.object(http_transport.http.client.HTTPSConnection, 'connect') as tls):
            try:
                # Fixed remaining time isolates slot ownership from wall-clock
                # scheduling; other tests retain the real attempt deadline.
                deadline = SimpleNamespace(target=http_transport.RequestTarget('yahoo.splits'),
                                           remaining=lambda: .05)
                with self.assertRaisesRegex(TimeoutError, 'DNS exceeded'):
                    http_transport.connect_resolved(deadline, ('query1.finance.yahoo.com', 443), .05)
                self.assertTrue(entered.is_set())
                for endpoint, host in (_A54_TARGETS[2], _A54_TARGETS[0]):
                    with self.subTest(occupied_target=endpoint):
                        with http_transport.Deadline(20, endpoint=endpoint) as deadline:
                            with self.assertRaisesRegex(AuthorityError, 'still occupied'):
                                http_transport.connect_resolved(deadline, (host, 443), 20)
                self.assertEqual(lookups, ['query1.finance.yahoo.com'])
                self.assertEqual(connected, [])
                opened.assert_not_called()
                tls.assert_not_called()
                http_open.assert_not_called()
            finally:
                release.set()
                self.assertTrue(slot.acquire(timeout=3), 'actual resolver must finish before fresh lookup')
                slot.release()
            with http_transport.Deadline(20, endpoint='sec.tickers') as fresh:
                self.assertIs(http_transport.connect_resolved(fresh, ('www.sec.gov', 443), 20), sock)
            sock.close()
            self.assertEqual(lookups, ['query1.finance.yahoo.com', 'www.sec.gov'])
            self.assertEqual(connected, [('127.0.0.1', 443)])
            self.assertEqual(closed, [True])
            opened.assert_called_once()
            tls.assert_not_called()
            http_open.assert_not_called()
        for endpoint in ('sec.tickers', 'sec.companyfacts', 'stockanalysis.history'):
            with self.subTest(busy_guard=endpoint):
                self.a54_dns_busy_witness(endpoint)
        mutant = _source_inverse(http_transport.resolve,
            'if not _DNS_SLOT.acquire(blocking=False):', 'if False:')
        with patch.object(http_transport, 'resolve', mutant):
            for endpoint in ('sec.tickers', 'sec.companyfacts', 'stockanalysis.history'):
                with self.subTest(busy_inverse=endpoint):
                    with self.assertRaises(AssertionError):
                        self.a54_dns_busy_witness(endpoint)
                    print('A54_DNS_BUSY_INVERSE_RED', endpoint)
        self.a52_root_dns_cross_host_witness()
        for root in ('single', 'batch', 'cli'):
            for label, old, new in (
                    ('busy-refusal', 'if not _DNS_SLOT.acquire(blocking=False):', 'if False:'),
                    ('early-release',
                     'raise TimeoutError(f"{target.label} DNS exceeded total attempt deadline")',
                     '_DNS_SLOT.release()\n        raise TimeoutError(f"{target.label} DNS exceeded total attempt deadline")')):
                with self.subTest(root_dns_inverse=root, guard=label):
                    inverse = _source_inverse(http_transport.resolve, old, new)
                    with patch.object(http_transport, 'resolve', inverse):
                        with self.assertRaisesRegex(AssertionError, 'A52 busy DNS is root STOP'):
                            self.a52_root_dns_cross_host_witness(seed_root=root, inverse_slot=True)
                    print('A52_ROOT_DNS_INVERSE_RED', root, label)
            with self.subTest(root_late_dns=root):
                self.a52_root_late_dns_witness(root)
                inverse = _source_inverse(http_transport.resolve,
                    'deadline.remaining()  # A late result is never permission to connect.', 'pass')
                with patch.object(http_transport, 'resolve', inverse):
                    with self.assertRaisesRegex(AssertionError, 'A52 late DNS never begins socket construction'):
                        self.a52_root_late_dns_witness(root)
                print('A52_ROOT_DNS_INVERSE_RED', root, 'late-result')
            with self.subTest(root_catalog_dns=root):
                self.a52_root_catalog_dns_witness(root)
                inverse = _source_inverse(http_transport._lookup_addresses,
                    'socket.getaddrinfo(target.host, 443,',
                    'socket.getaddrinfo(__import__("urllib.parse", fromlist=["urlsplit"]).urlsplit('
                    '"https://url-host.example.invalid").hostname, 443,')
                with patch.object(http_transport, '_lookup_addresses', inverse):
                    with self.assertRaisesRegex(AssertionError, 'A52 root SEC resolver uses catalog host, not URL text'):
                        self.a52_root_catalog_dns_witness(root)
                print('A52_ROOT_DNS_INVERSE_RED', root, 'catalog-host')

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
        for endpoint, host in _A54_TARGETS:
            with self.subTest(endpoint=endpoint):
                self.a54_resolved_connect_witness(endpoint, host)

    def a54_resolved_connect_witness(self, endpoint='stockanalysis.history',
                                   host='stockanalysis.com'):
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
            with http_transport.Deadline(20, endpoint=endpoint) as deadline:
                actual = http_transport.connect_resolved(deadline, (host, 443), 20)
                self.assertIs(actual, sock)
                self.assertEqual(timeouts, [20, 17], 'TLS must inherit only the remaining budget')
                self.assertEqual(effects, [('127.0.0.1', 443)])
            sock.close()
            with http_transport.Deadline(2, endpoint=endpoint) as deadline, self.assertRaises(TimeoutError):
                http_transport.connect_resolved(deadline, (host, 443), 2)
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
        deadline.target = http_transport.RequestTarget('stockanalysis.history')
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
        deadline.target = http_transport.RequestTarget('stockanalysis.history')
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

    def a54_dns_owner_witness(self, endpoint):
        slot, pending = threading.BoundedSemaphore(1), []
        deadline = Mock(target=http_transport.RequestTarget(endpoint))
        deadline.remaining.return_value = 20
        class Event:
            def wait(_self, seconds):
                return False
            def set(_self):
                pass
        class Thread:
            def __init__(_self, *, target, args, **kwargs):
                _self.run = lambda: target(*args)
            def start(_self):
                pending.append(_self.run)
        with (patch.object(http_transport, '_DNS_SLOT', slot),
              patch.object(http_transport.threading, 'Event', Event),
              patch.object(http_transport.threading, 'Thread', Thread),
              patch.object(http_transport, '_lookup_addresses', return_value=[
                  (2, 1, 6, '', ('127.0.0.1', 443))]),
              patch.object(http_transport, '_make_socket') as opened):
            with self.assertRaisesRegex(TimeoutError, 'DNS exceeded'):
                http_transport.resolve(deadline)
            self.assertEqual(len(pending), 1)
            released_early = slot.acquire(blocking=False)
            try:
                self.assertFalse(released_early, 'only the late resolver may release its slot')
            finally:
                # A wrong early release was acquired above, so the queued worker
                # still balances exactly one release even in that red mutant.
                pending.pop()()
            self.assertTrue(slot.acquire(blocking=False))
            slot.release()
            opened.assert_not_called()

    def a64_transport_guard_witness(self, case, endpoint='stockanalysis.history'):
        deadline = Mock()
        deadline.target = http_transport.RequestTarget(endpoint)
        deadline.remaining.return_value = 20
        host = deadline.target.host
        if case == 'host':
            with self.assertRaisesRegex(AuthorityError, 'TLS host differs'):
                http_transport.DeadlineHTTPSConnection('example.com', deadline=deadline,
                                                       context=ssl.create_default_context())
        elif case in ('tunnel', 'tls-rearm'):
            connection = http_transport.DeadlineHTTPSConnection(host, deadline=deadline,
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
            def lookup(target):
                self.assertEqual(target.endpoint_id, endpoint)
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
        elif case == 'dns-target':
            with patch.object(http_transport.socket, 'getaddrinfo', return_value=[]) as lookup:
                self.assertEqual(http_transport._lookup_addresses(deadline.target), [])
                lookup.assert_called_once_with(host, 443, type=http_transport.socket.SOCK_STREAM,
                                               proto=http_transport.socket.IPPROTO_TCP)
        elif case == 'dns-owner':
            self.a54_dns_owner_witness(endpoint)
        elif case in ('target-subclass', 'target-revalidation'):
            class Subclass(http_transport.RequestTarget):
                @property
                def host(_self):
                    return 'other.invalid'
            if case == 'target-subclass':
                deadline.target = Subclass(endpoint)
                expected = 'immutable catalog selector'
            else:
                deadline.target = http_transport.RequestTarget(endpoint)
                object.__setattr__(deadline.target, 'endpoint_id', 'unregistered.membership')
                expected = 'finite catalog'
            with (patch.object(http_transport, '_lookup_addresses') as lookup,
                  patch.object(http_transport, '_make_socket') as opened):
                with self.assertRaisesRegex(AuthorityError, expected):
                    http_transport.resolve(deadline)
                lookup.assert_not_called()
                opened.assert_not_called()
        elif case == 'target-policy':
            self.assertEqual(host, dict(_A54_TARGETS)[endpoint])
            with self.assertRaises(FrozenInstanceError):
                deadline.target.endpoint_id = 'stockanalysis.history'
            with http_transport.Deadline(20, endpoint=endpoint) as actual:
                self.assertEqual(actual.target, deadline.target)
                with self.assertRaises(AttributeError):
                    actual.target = SimpleNamespace(host='other.invalid')
            with patch.object(http_transport.threading, 'Timer') as timer:
                for invalid in ('unregistered.github', 'unregistered.wikipedia', 'query2.finance.yahoo.com',
                                'https://www.sec.gov/', '', None, SimpleNamespace(host=host)):
                    with self.subTest(invalid=type(invalid).__name__):
                        with self.assertRaisesRegex(AuthorityError, 'finite catalog'):
                            http_transport.Deadline(20, endpoint=invalid)
                timer.assert_not_called()
                altered = dict(ENDPOINTS)
                altered[endpoint] = replace(altered[endpoint], hosts=('other.invalid',))
                with patch.object(http_transport, 'ENDPOINTS', altered):
                    with self.assertRaisesRegex(AuthorityError, 'primary catalog host differs'):
                        http_transport.Deadline(20, endpoint=endpoint)
                timer.assert_not_called()
            deadline.target = SimpleNamespace(host=host, endpoint_id=endpoint)
            with (patch.object(http_transport, '_lookup_addresses') as lookup,
                  patch.object(http_transport, '_make_socket') as opened):
                for route in ('dns', 'tcp', 'tls'):
                    with self.subTest(route=route):
                        with self.assertRaisesRegex(AuthorityError, 'immutable catalog selector'):
                            if route == 'dns':
                                http_transport.resolve(deadline)
                            elif route == 'tcp':
                                http_transport.connect_resolved(deadline, (host, 443), 20)
                            else:
                                http_transport.DeadlineHTTPSConnection(host, deadline=deadline,
                                    context=ssl.create_default_context())
                lookup.assert_not_called()
                opened.assert_not_called()
        else:
            addresses = [(2, 1, 6, '', ('127.0.0.1', 443))]
            target = (host, 443)
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
        for endpoint, _host in _A54_TARGETS:
            for case in ('target', 'host', 'tunnel', 'tls-rearm', 'dns-count',
                         'dns-family', 'dns-port', 'dns-context', 'dns-target',
                         'dns-owner', 'target-policy', 'target-subclass', 'target-revalidation'):
                with self.subTest(endpoint=endpoint, case=case):
                    self.a64_transport_guard_witness(case, endpoint)
        self.a64_injection_witness(False)
        self.a64_injection_witness(True)
        self.a64_cancel_witness()

    def test_a64_guard_inverses(self):
        cases = (
            (http_transport, '_checked_target',
             'if type(target) is not RequestTarget:',
             'if not isinstance(target, RequestTarget):', 'target-subclass'),
            (http_transport, '_checked_target', 'target.__post_init__()', 'pass', 'target-revalidation'),
            (http_transport, 'connect_resolved',
             'if address != (target_policy.host, 443) or source_address is not None:', 'if False:', 'target'),
            (http_transport.DeadlineHTTPSConnection, '__init__',
             'if self.host != target.host or self.port != 443:', 'if False:', 'host'),
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
            (http_transport, '_lookup_addresses', 'socket.getaddrinfo(target.host, 443,',
             'socket.getaddrinfo(_HOST, 443,', 'dns-target'),
            (http_transport, 'resolve',
             'raise TimeoutError(f"{target.label} DNS exceeded total attempt deadline")',
             '_DNS_SLOT.release()\n        raise TimeoutError(f"{target.label} DNS exceeded total attempt deadline")',
             'dns-owner'),
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
                for endpoint in ('sec.tickers', 'sp500.github', 'sp500.wikipedia'):
                    with self.subTest(endpoint=endpoint), patch.object(owner, name, mutant):
                        with self.assertRaises(AssertionError):
                            self.a64_transport_guard_witness(case, endpoint)
                    print('A64_GUARD_INVERSE_RED', case, endpoint)
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
        deadline.target = http_transport.RequestTarget('stockanalysis.history')
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
            target = http_transport.RequestTarget('stockanalysis.history')
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
             'a54_resolved_connect_witness'),
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

    def a65_consumer_calendar_witness(self, consumer, phase):
        """Real roots/consumers/default open, with only isolated fixture I/O."""
        self.assertIn(consumer, ('validation', 'sweep'))
        captured = {'unsettled': '2026-08-04T13:20:00',
                    'settled': '2026-08-04T16:20:00',
                    'weekend': '2026-08-08T13:20:00'}[phase]
        rows = dict(self.authority.rows)
        day = date(2026, 8, 4)
        self.assertEqual(rows[day].close, time(16))
        rows[day] = replace(rows[day], status='open_early_close', close=time(13))
        authority = replace(self.authority, rows=rows)
        project = Path(tempfile.mkdtemp(dir=self.temp.name, prefix='a65-' + consumer + '-'))
        bank, logs = project / 'bank', project / 'Run Logs'
        directory, cache = logs / 'ledgers', logs / 'cache'
        history = []
        for offset in range(12):
            month = 8 + offset
            label = date(2025 + (month - 1) // 12, (month - 1) % 12 + 1, 3)
            while authority.window('1d', label) is None:
                label += timedelta(days=1)
            history.append(label.isoformat())
        labels_seen = history + ['2026-08-03', '2026-08-04', '2026-08-07', '2026-08-10']
        raw = json.dumps({'data': [self.row(label) for label in labels_seen]}).encode()
        expected = history + ['2026-08-03', '2026-08-04']
        if phase == 'weekend':
            expected.append('2026-08-07')
        snapshot = {'ticker': 'TEST', 'conid': 101, 'provider_symbol': 'TEST',
                    'manifest_fingerprint': 'a' * 64,
                    'stored': {label: 10.0 for label in history + ['2026-08-03']},
                    'stored_rows': 13, 'stored_first': history[0],
                    'stored_last': '2026-08-03', 'stored_basis': 'raw',
                    'basis_actions_applied': []}
        cache_path = external_sweep._cache_path(cache, snapshot)
        cache_path.parent.mkdir(parents=True)
        cache_path.write_bytes(b'last-good-cache sentinel\n')
        original_cache = cache_path.read_bytes()
        stock_validate.clear_reference_cache()
        old_reference = {'last-good': 'a65 sentinel'}
        stock_validate._REF_CACHE[('TEST', 'Max')] = deepcopy(old_reference)
        opens, turns, publications, responses, reached = [], [], [], [], []
        governor = self.governors.provider('stockanalysis.history')
        original_turn = governor.wait_turn
        original_publish = stock_validate._publish_reference_cache
        original_sidecar = stock_validate.save_daily_reference
        original_write = external_sweep.write_cache

        def opened(_opener, wire, *, timeout):
            stack = {frame.function for frame in inspect.stack()}
            required = ({'fetch_daily_reference'} if consumer == 'validation'
                        else {'_provider_fetch', '_read'})
            self.assertTrue(required <= stack, 'calendar witness must reach the real consumer')
            self.assertEqual(wire.full_url, stockanalysis_url('TEST', 'Max'))
            opens.append(wire.full_url)
            reached.append(required)
            response = io.BytesIO(raw)
            responses.append(response)
            return response

        def turn(*args, **kwargs):
            turns.append(True)
            return original_turn(*args, **kwargs)

        def sealed():
            paths = list(directory.glob('*.jsonl'))
            self.assertEqual(len(paths), 1)
            return inspect_ledger(paths[0])

        def published(pending, context):
            self.assertTrue(sealed()['verified'])
            self.assertEqual([row['label'] for _, entry in pending for row in entry['rows']], expected)
            publications.append('reference')
            return original_publish(pending, context)

        def sidecar(root, ticker, rng, reference, **kwargs):
            self.assertTrue(sealed()['verified'])
            self.assertEqual(sorted(reference), expected)
            publications.append('sidecar')
            return original_sidecar(root, ticker, rng, reference, **kwargs)

        def cache_write(target, root, identity, payload):
            self.assertTrue(sealed()['verified'])
            self.assertEqual([row['label'] for row in payload['accepted_rows']], expected)
            self.assertEqual(sorted(payload['reference']), expected)
            publications.append('sweep-cache')
            return original_write(target, root, identity, payload)

        caught = None
        with (tp.offline_policy() as capability,
              patch.object(urllib.request.OpenerDirector, 'open', opened),
              patch.object(governor, 'wait_turn', turn),
              patch.object(stock_validate, '_publish_reference_cache', published),
              patch.object(stock_validate, 'save_daily_reference', sidecar),
              patch.object(external_sweep, 'stored_daily_snapshot', return_value=snapshot),
              patch.object(external_sweep, '_manifest_unchanged', return_value=True),
              patch.object(external_sweep, 'write_cache', cache_write)):
            options = dict(_test_capability=capability, _evidence_dir=directory,
                           _authority=authority, _governors=self.governors,
                           _clock=lambda: datetime.fromisoformat(captured).replace(tzinfo=NY))
            try:
                if consumer == 'validation':
                    bars = [(datetime(2026, 8, 3), 10.0, 11.0, 9.0, 10.0, 100)]
                    stock_validate.validate_series(bank, 'TEST', '1d', rng='Max',
                        read_fn=lambda *_: bars, save_reference=True, **options)
                else:
                    external_sweep.sweep_ticker(bank, 'TEST',
                        provider=external_sweep.StockAnalysisProvider(), cache_root=cache,
                        asof=datetime.fromisoformat(captured).replace(tzinfo=NY), **options)
            except RequestRefused as error:
                caught = error
        if phase == 'unsettled':
            # First assertion is the inverse's intended physical-send effect.
            self.assertEqual(opens, [], 'refused producer must have zero physical opens')
            self.assertEqual(turns, [], 'refused producer must have zero governor turns')
            self.assertIsInstance(caught, RequestRefused)
            self.assertIn('range-less HTTP requires daily settlement', str(caught))
            self.assertEqual(publications, [])
            self.assertEqual(stock_validate._REF_CACHE[('TEST', 'Max')], old_reference)
            self.assertEqual(cache_path.read_bytes(), original_cache)
            self.assertFalse((bank / 'TEST').exists())
            self.assertFalse((project / 'Validation Reference').exists())
            paths = list(directory.glob('*.jsonl'))
            self.assertEqual(len(paths), 1)
            audit = inspect_ledger(paths[0], require_seal=False)
            self.assertFalse(audit['verified'])
            self.assertEqual(audit['events'][0]['payload']['reason'],
                             'range-less HTTP requires daily settlement')
        else:
            self.assertIsNone(caught)
            self.assertEqual(len(opens), 1)
            self.assertEqual(turns, [True])
            self.assertEqual(publications, ['reference', 'sidecar'] if consumer == 'validation'
                             else ['sweep-cache'])
            if consumer == 'validation':
                self.assertEqual([row['label'] for row in stock_validate._REF_CACHE[('TEST', 'Max')]['rows']], expected)
                sidecars = list((project / 'Validation Reference').glob('TEST_daily_Max_asof-*.csv'))
                self.assertEqual(len(sidecars), 1, 'successful calendar control must publish the real sidecar')
                self.assertEqual([line.split(',')[0] for line in sidecars[0].read_text().splitlines()[1:]], expected)
            else:
                self.assertNotEqual(cache_path.read_bytes(), original_cache,
                                    'successful calendar control must replace the isolated cache sentinel')
                cached = json.loads(cache_path.read_text(encoding='utf-8'))
                self.assertEqual([row['label'] for row in cached['accepted_rows']], expected)
            audit = sealed()
            self.assertEqual([event['event'] for event in audit['events']], ['decision', 'result', 'seal'])
            self.assertEqual(audit['events'][0]['producer_id'], 'http.stockanalysis.' +
                             ('validation' if consumer == 'validation' else 'external_sweep'))
            evidence = audit['events'][1]['payload']
            self.assertEqual(evidence['raw_response_digest'], hashlib.sha256(raw).hexdigest())
            self.assertEqual(evidence['observed']['count'], len(labels_seen))
            self.assertEqual(evidence['accepted']['count'], len(expected))
            self.assertEqual(evidence['dropped'], len(labels_seen) - len(expected))
            self.assertTrue(all(response.closed for response in responses))
        print('A65_CONSUMER_CALENDAR', consumer, phase, 'opens', len(opens),
              'turns', len(turns), 'publications', publications)

    def test_a65_false_early_close_both_consumer_roots(self):
        for consumer in ('validation', 'sweep'):
            for phase in ('unsettled', 'settled', 'weekend'):
                with self.subTest(consumer=consumer, phase=phase):
                    self.a65_consumer_calendar_witness(consumer, phase)

    def test_a65_tabled_close_only_inverse_is_red_for_both_consumers(self):
        mutant = _source_inverse(ScheduleAuthority.daily_settled_close,
                                 'max(row.close, REGULAR_CLOSE)', 'row.close')
        for consumer in ('validation', 'sweep'):
            with self.subTest(consumer=consumer):
                with patch.object(ScheduleAuthority, 'daily_settled_close', mutant):
                    with self.assertRaisesRegex(AssertionError, 'refused producer must have zero physical opens'):
                        self.a65_consumer_calendar_witness(consumer, 'unsettled')
                print('A65_SETTLE_FLOOR_INVERSE_RED', consumer)

    def a66_bounds_witness(self):
        for when, cutoff in (('2026-08-04T17:00:00', '2021-08-04'),
                             ('2024-02-29T17:00:00', '2019-02-28')):
            context = self.context(when)
            maximum = labels.stockanalysis_bounds(context.authority, context.captured_now, 'Max')
            five = labels.stockanalysis_bounds(context.authority, context.captured_now, '5Y')
            self.assertEqual(maximum['intended_start'].date(), context.authority.first_date,
                             'Max must start at coverage, not before it')
            self.assertEqual(five['requested_start_date'], cutoff,
                             '5Y must use five civil years from the captured clock')
            self.assertEqual(five['intended_end'], context.horizons['1d'])

    def test_a66_actual_bounds_and_source_mutations(self):
        self.a66_bounds_witness()
        for name, old, new, reason in (
            ('max-before-coverage', '"intended_start": start,',
             '"intended_start": start - timedelta(days=1),', 'Max must start at coverage'),
            ('rolling-365', 'requested = today.replace(year=today.year - 5)',
             'requested = today - timedelta(days=5 * 365)', '5Y must use five civil years'),
            ('second-clock', 'today = captured_now.date()',
             'today = datetime.now(timezone.utc).date()', '5Y must use five civil years'),
        ):
            with self.subTest(mutant=name):
                mutant = _source_inverse(labels.stockanalysis_bounds, old, new)
                # A deterministic second clock, not a dependence on wall-clock date.
                calls = []
                class SecondClock(datetime):
                    @classmethod
                    def now(cls, tz=None):
                        calls.append(tz)
                        return datetime(2026, 8, 5, tzinfo=timezone.utc)
                with (patch.object(labels, 'stockanalysis_bounds', mutant),
                      patch.object(labels, 'datetime', SecondClock)):
                    with self.assertRaisesRegex(AssertionError, reason):
                        self.a66_bounds_witness()
                self.assertEqual(len(calls), 1 if name == 'second-clock' else 0)
                print('A66_BOUNDS_SOURCE_INVERSE_RED', name)

    def a66_filter_witness(self, extra_label):
        with tp.offline_policy() as capability:
            context = self.context(capability=capability)
            accepted, _ = self.execute(context, [self.row('2026-08-03'), self.row(extra_label)], rng='Max')
            context.seal()
        self.assertEqual([row['label'] for row in accepted], ['2026-08-03'],
                         'closed and out-of-coverage labels must stay observed only')
        evidence = inspect_ledger(context.ledger.path)['events'][1]['payload']
        self.assertEqual((evidence['observed']['count'], evidence['accepted']['count'], evidence['dropped']),
                         (2, 1, 1))

    def test_a66_closed_and_unsupported_source_bypass_inverses(self):
        for label in ('2026-08-01', '1979-12-31'):
            with self.subTest(label=label):
                self.a66_filter_witness(label)
                # Mutate the shipped filter to release this rejected label before
                # its redundant membership, coverage, settlement and bounds checks.
                mutant = _source_inverse(labels.filter_stockanalysis_rows,
                    'for row in rows:', 'for row in rows:\n'
                    f'        if row["label"] == {label!r}:\n'
                    '            accepted.append({**row, "timestamp": "2026-08-03T20:00:00+00:00"})\n'
                    '            continue')
                with patch.object(labels, 'filter_stockanalysis_rows', mutant):
                    with self.assertRaisesRegex(AssertionError, 'closed and out-of-coverage labels must stay observed only'):
                        self.a66_filter_witness(label)
                print('A66_FILTER_SOURCE_BYPASS_INVERSE_RED', label,
                      'combined membership/coverage/settlement/bounds bypass, not a single-guard claim')

    def test_a66_label_shift_and_midnight_source_inverses(self):
        self.test_original_iso_and_epoch_labels_use_authority_close()
        normalizer = _source_inverse(labels._normalize_stockanalysis_rows_strict,
            'label = _norm_date_key(source_date)',
            'label = (date.fromisoformat(_norm_date_key(source_date)) - timedelta(days=1)).isoformat()')
        with patch.object(labels, '_normalize_stockanalysis_rows_strict', normalizer):
            with self.assertRaisesRegex(AssertionError, 'Lists differ'):
                self.test_original_iso_and_epoch_labels_use_authority_close()
        print('A66_LABEL_SOURCE_INVERSE_RED original ISO/epoch date shifted one day')
        midnight = _source_inverse(labels.filter_stockanalysis_rows,
            'accepted.append({**row, "timestamp": close.astimezone(timezone.utc).isoformat()})',
            'if context.authority.accepts_label("1d", close, context.captured_now):\n'
            '            accepted.append({**row, "timestamp": close.astimezone(timezone.utc).isoformat()})')
        with patch.object(labels, 'filter_stockanalysis_rows', midnight):
            with self.assertRaisesRegex(AssertionError, 'Lists differ'):
                self.test_original_iso_and_epoch_labels_use_authority_close()
        print('A66_MIDNIGHT_SOURCE_INVERSE_RED authority-derived close misapplied to old midnight rule')

    def a67_static_call_catalog(self, sources=None):
        root = Path(__file__).resolve().parents[1]
        catalog = {}
        selected = {row[:4] for row in _A67_STATIC_EDGES}
        for path in sorted({row[0] for row in _A67_STATIC_EDGES}):
            source = (root / path).read_text(encoding='utf-8') if sources is None else sources[path]
            stack = []
            class Calls(ast.NodeVisitor):
                def scoped(_visitor, node):
                    stack.append(node.name)
                    _visitor.generic_visit(node)
                    stack.pop()
                visit_ClassDef = scoped
                visit_FunctionDef = scoped
                visit_AsyncFunctionDef = scoped
                def visit_Call(_visitor, node):
                    owner = '.'.join(stack) or '<module>'
                    key = (path, owner, node.lineno, ast.unparse(node.func))
                    if key in selected:
                        self.assertNotIn(key, catalog, 'selected source call key must be unique')
                        catalog[key] = node
                    _visitor.generic_visit(node)
            Calls().visit(ast.parse(source))
        return catalog

    def a67_forward_call(self, binding, node, source=None):
        path, owner, line, expression, argc, keywords, group = binding
        self.assertEqual((len(node.args), tuple(kw.arg for kw in node.keywords)), (argc, keywords),
                         'static forwarding argument contract changed')
        if (path, owner, expression) == ('engine/split_cache.py', 'main', 'run_refresh_cli'):
            self.assertFalse({'_fetch_worker', '_fetch_child'} & set(keywords),
                             'UI/CLI forwarding must not mint or inject offline authority')
            forwarded_capability = next(kw.value for kw in node.keywords if kw.arg == '_test_capability')
            self.assertEqual(ast.dump(forwarded_capability), ast.dump(ast.Name(id='_test_capability', ctx=ast.Load())),
                             'A67 engine main only forwards its explicit offline fixture input')
            source = source if source is not None else (Path(__file__).resolve().parents[1] / path).read_text(encoding='utf-8')
            main = next(item for item in ast.parse(source).body
                        if isinstance(item, ast.FunctionDef) and item.name == 'main')
            defaults = dict(zip((arg.arg for arg in main.args.kwonlyargs), main.args.kw_defaults))
            self.assertEqual(ast.dump(defaults['_test_capability']), ast.dump(ast.Constant(value=None)),
                             'A67 engine fixture authority defaults to None')
            command_flags = [arg.value for call in ast.walk(main) if isinstance(call, ast.Call)
                and isinstance(call.func, ast.Attribute) and call.func.attr == 'add_argument'
                for arg in call.args if isinstance(arg, ast.Constant) and isinstance(arg.value, str)]
            self.assertFalse(any('capability' in flag for flag in command_flags),
                             'A67 CLI cannot select offline fixture authority')
        else:
            self.assertFalse({'_fetch_worker', '_fetch_child', '_test_capability'} & set(keywords),
                             'UI/CLI forwarding must not mint or inject offline authority')
        # Evaluate only this shipped call expression, never its containing module,
        # wrapper, thread, callback or application. All dependencies are memory
        # doubles; the leaf merely captures its forwarded arguments.
        class Value:
            def __init__(_self, name):
                _self.name = name
            def __getattr__(_self, name):
                return Value(_self.name + '.' + name)
            def __call__(_self, *args, **kwargs):
                return Value(_self.name + '()')
            def __iter__(_self):
                return iter((Value(_self.name + '[0]'),))
            def __str__(_self):
                return _self.name
            def __bool__(_self):
                return False
        names = {node.id for node in ast.walk(node) if isinstance(node, ast.Name)}
        namespace = {name: Value(name) for name in names}
        namespace.update({'__builtins__': {'str': str, 'tuple': tuple, 'list': list}, 'kwargs': {}})
        forwarded = Mock(return_value=Value('detached-result'))
        parts = expression.split('.')
        if len(parts) == 1:
            namespace[expression] = forwarded
        else:
            target = namespace[parts[0]]
            for part in parts[1:-1]:
                child = Value(part)
                setattr(target, part, child)
                target = child
            setattr(target, parts[-1], forwarded)
        result = eval(compile(ast.Expression(deepcopy(node)), path, 'eval'), namespace)
        self.assertEqual(result.name, 'detached-result')
        forwarded.assert_called_once()
        self.assertEqual(len(forwarded.call_args.args), argc)
        self.assertEqual(set(forwarded.call_args.kwargs), {kw for kw in keywords if kw is not None})
        # Archives are inspected only; this never calls their real repair roots.
        print('A67_SOURCE_FORWARDING', group, path, owner, line, expression)

    def test_a67_exact_static_edge_forwarding_without_imports(self):
        self.assertEqual(len(_A67_STATIC_EDGES), 35)
        keys = {row[:4] for row in _A67_STATIC_EDGES}
        self.assertEqual(len(keys), 35)
        catalog = self.a67_static_call_catalog()
        for binding in _A67_STATIC_EDGES:
            with self.subTest(path=binding[0], owner=binding[1], line=binding[2]):
                self.assertIn(binding[:4], catalog, 'static edge must match exact source owner/site')
                self.a67_forward_call(binding, catalog[binding[:4]])
        self.assertEqual({row[6] for row in _A67_STATIC_EDGES},
                         {'validation-gui', 'export-health', 'split-entry', 'cli-entry'})

    def test_a67_forwarding_shape_inverses(self):
        catalog = self.a67_static_call_catalog()
        for binding in _A67_STATIC_EDGES:
            if not binding[4]:
                continue
            mutant = deepcopy(catalog[binding[:4]])
            mutant.args.pop(0)
            with self.subTest(site=binding[:4]):
                with self.assertRaisesRegex(AssertionError, 'static forwarding argument contract changed'):
                    self.a67_forward_call(binding, mutant)
        binding = next(row for row in _A67_STATIC_EDGES if row[6] == 'validation-gui' and row[4])
        mutant = deepcopy(catalog[binding[:4]])
        mutant.keywords.append(ast.keyword(arg='_test_capability', value=ast.Constant(None)))
        with self.assertRaisesRegex(AssertionError, 'static forwarding argument contract changed'):
            self.a67_forward_call(binding, mutant)
        print('A67_SOURCE_FORWARDING_INVERSES_RED missing argument and unreviewed authority injection')
        binding = next(row for row in _A67_STATIC_EDGES
                       if row[:2] == ('engine/split_cache.py', 'main'))
        mutant = deepcopy(catalog[binding[:4]])
        next(kw for kw in mutant.keywords if kw.arg == '_test_capability').value = ast.Constant(value=None)
        with self.assertRaisesRegex(AssertionError, 'A67 engine main only forwards its explicit offline fixture input'):
            self.a67_forward_call(binding, mutant)
        print('A67_ENGINE_FORWARD_INVERSE_RED fixture-input substitution')
        source = (Path(__file__).resolve().parents[1] / binding[0]).read_text(encoding='utf-8')
        old = 'sleep_fn=time.sleep, _test_capability=None'
        self.assertEqual(source.count(old), 1)
        with self.assertRaisesRegex(AssertionError, 'A67 engine fixture authority defaults to None'):
            self.a67_forward_call(binding, catalog[binding[:4]], source.replace(old,
                'sleep_fn=time.sleep, _test_capability=True', 1))
        print('A67_ENGINE_FORWARD_INVERSE_RED fixture-default substitution')
        old = 'parser.add_argument("--timeout", type=int, default=30)'
        self.assertEqual(source.count(old), 1)
        with self.assertRaisesRegex(AssertionError, 'A67 CLI cannot select offline fixture authority'):
            self.a67_forward_call(binding, catalog[binding[:4]], source.replace(old,
                'parser.add_argument("--test-capability", type=int, default=30)', 1))
        print('A67_ENGINE_FORWARD_INVERSE_RED CLI-authority flag')

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


    # A separate literal oracle, not derived from the bridge's binding table.
    _A23B_BINDINGS = (
        ('http.split_provider.yahoo', 'yahoo.splits', 'http-series', 'TEST'),
        ('http.split_provider.sec_tickers', 'sec.tickers', 'http-metadata', 'SEC:TICKERS'),
        ('http.split_provider.sec_facts', 'sec.companyfacts', 'http-metadata', 'CIK:0000789019'),
        ('http.split_provider.issuer', 'fortinet.history', 'http-metadata', 'FTNT'),
    )

    def a59_split_fixture(self, producer):
        if producer.endswith('yahoo'):
            return json.dumps({'chart': {'result': [{'events': {'splits': {
                'accepted': {'date': int(datetime(2026, 8, 3, tzinfo=timezone.utc).timestamp()),
                             'numerator': 2, 'denominator': 1}}}}], 'error': None}}).encode()
        if producer.endswith('sec_tickers'):
            return b'{"0":{"ticker":"TEST","cik_str":789019}}'
        if producer.endswith('sec_facts'):
            return b'{"cik":789019,"facts":{}}'
        return b'<html>The second time. In 2011 a two-for-one stock split.</html>'

    def a59_split_options(self, producer):
        if producer.endswith('sec_facts'):
            return {'cik': 789019, 'sec_user_agent': 'Offline fixture fixture@example.invalid'}
        if producer.endswith('sec_tickers'):
            return {'sec_user_agent': 'Offline fixture fixture@example.invalid'}
        return {}

    def test_a52_split_endpoint_binding_matrix(self):
        source_tree = ast.parse(Path(__file__).read_text(encoding='utf-8'))
        suite = next(node for node in source_tree.body
                     if isinstance(node, ast.ClassDef) and node.name == 'HttpLabels')
        names = [node.name for node in suite.body
                 if isinstance(node, ast.FunctionDef) and node.name.startswith('test_')]
        self.assertEqual(len(names), len(set(names)), 'shadowed duplicate test definitions')
        site_key = 'engine.fetch_http_splits:guarded_split_attempt.one_send:http_opener_open:1'
        inventory = fetch_send_inventory.build_inventory()
        physical = next(row for row in inventory['sites'] if row['site_key'] == site_key)
        self.assertEqual((physical['producer_id'], physical['transport'], physical['disposition']),
                         ('http.split_provider.owned_default', 'http-mixed', 'a2_refused'))
        for producer, endpoint, variant, subject in self._A23B_BINDINGS:
            symbol = 'FTNT' if endpoint == 'fortinet.history' else 'TEST'
            options = self.a59_split_options(producer)
            for default in (False, True):
                with self.subTest(producer=producer, default=default), tp.offline_policy() as capability:
                    context = self.context(capability=capability)
                    worker = context.worker('a59-binding')
                    raw = self.a59_split_fixture(producer)
                    opens, responses = [], []
                    def wire_open(_opener, wire, *, timeout):
                        frame = inspect.currentframe().f_back
                        self.assertEqual(frame.f_code.co_qualname, 'guarded_split_attempt.<locals>.one_send')
                        self.assertEqual(frame.f_lineno, physical['line'])
                        self.assertGreater(timeout, 0)
                        self.assertEqual(wire.get_method(), 'GET')
                        opens.append(wire.full_url)
                        events = inspect_ledger(context.ledger.path, require_seal=False)['events']
                        self.assertEqual(len(events), 1, 'decision must be durable before physical open')
                        decision = events[0]['payload']
                        self.assertEqual(decision['http_request']['url'], wire.full_url)
                        for name, digest in decision['http_request']['header_digests'].items():
                            self.assertEqual(hashlib.sha256(wire.get_header(name.capitalize()).encode()).hexdigest(), digest)
                        response = io.BytesIO(raw)
                        responses.append(response)
                        return response
                    def injected(url):
                        opens.append(url)
                        self.assertEqual(len(inspect_ledger(context.ledger.path, require_seal=False)['events']), 1)
                        return raw
                    with patch.object(urllib.request.OpenerDirector, 'open', wire_open):
                        accepted = splits.guarded_split_attempt(worker, producer, symbol,
                            None if default else injected, **options)
                    context.seal()
                    events = inspect_ledger(context.ledger.path)['events']
                    self.assertEqual(len(opens), 1)
                    self.assertEqual(len(events), 3)
                    decision, result = events[0]['payload'], events[1]['payload']
                    self.assertEqual((decision['requested']['endpoint'], decision['requested']['variant'],
                                      decision['requested']['subject']), (endpoint, variant, subject))
                    self.assertEqual(events[0]['producer_id'], producer)
                    self.assertEqual(result['raw_response_digest'], hashlib.sha256(raw).hexdigest())
                    self.assertTrue(accepted)
                    self.assertTrue(all(response.closed for response in responses))
                    if variant == 'http-metadata':
                        self.assertNotIn('intended_start', decision['requested'])
                        self.assertIn('metadata_exemption', decision['http_request'])
                    if options:
                        self.assertNotIn(options['sec_user_agent'].encode(), context.ledger.path.read_bytes())
            with self.subTest(producer=producer, default_policy='held'):
                context = self.context()
                with patch.object(urllib.request.OpenerDirector, 'open') as opener:
                    with self.assertRaises(RequestRefused):
                        splits.guarded_split_attempt(context.worker('a59-default-hold'), producer, symbol, **options)
                    opener.assert_not_called()
                self.assertEqual(inspect_ledger(context.ledger.path, require_seal=False)['events'], [])
        print('A59_BINDING four expansions; eight guarded physical positives; four default holds')
        self.a52_three_historical_events_witness()
        import split_provider
        inverse = _source_inverse(split_provider.YahooSecProvider.fetch_history,
            'for row in yahoo_rows:', 'for row in yahoo_rows[-1:]:')
        with patch.object(split_provider.YahooSecProvider, 'fetch_history', inverse):
            with self.assertRaisesRegex(AssertionError, 'A52 original three pre-2015 events retained'):
                self.a52_three_historical_events_witness()

    def a59_split_caller_witness(self, producer, endpoint):
        hosts = {'yahoo.splits': 'query1.finance.yahoo.com', 'sec.tickers': 'www.sec.gov',
                 'sec.companyfacts': 'data.sec.gov', 'fortinet.history': 'www.fortinet.com'}
        lookups = []
        def lookup(target):
            lookups.append(target.host)
            raise OSError('A59 actual split caller guarded DNS sentinel')
        def truncated_connect(connection):
            return http_transport.resolve(connection._deadline)
        with tp.offline_policy() as capability:
            context = self.context(capability=capability)
            with (patch.object(http_transport, '_lookup_addresses', lookup),
                  patch.object(http_transport, '_DNS_SLOT', threading.BoundedSemaphore(1)),
                  patch.object(http_transport, '_make_socket') as sockets,
                  patch.object(http_transport.DeadlineHTTPSConnection, 'connect', truncated_connect)):
                with self.assertRaises((URLError, AuthorityError)) as caught:
                    splits.guarded_split_attempt(context.worker('a59-actual-caller'), producer,
                        'FTNT' if endpoint == 'fortinet.history' else 'TEST', **self.a59_split_options(producer))
                sockets.assert_not_called()
            context.seal()
        self.assertEqual(lookups, [hosts[endpoint]], 'split caller target must reach its own guarded host')
        self.assertIsInstance(caught.exception, URLError)
        self.assertEqual(str(caught.exception.reason), 'A59 actual split caller guarded DNS sentinel')
        events = inspect_ledger(context.ledger.path)['events']
        self.assertEqual(len(events), 3)
        self.assertEqual(events[1]['payload']['outcome'], 'error')

    def test_a59_split_four_caller_targets_and_inverses(self):
        for producer, endpoint, _, _ in self._A23B_BINDINGS:
            with self.subTest(producer=producer):
                self.a59_split_caller_witness(producer, endpoint)
                mutant = _source_inverse(splits.guarded_split_attempt,
                    'with Deadline(timeout, endpoint=envelope.endpoint) as deadline:',
                    'with Deadline(timeout, endpoint="stockanalysis.history") as deadline:')
                with patch.object(splits, 'guarded_split_attempt', mutant):
                    with self.assertRaisesRegex(AssertionError, 'split caller target must reach its own guarded host'):
                        self.a59_split_caller_witness(producer, endpoint)
                print('A59_ACTUAL_CALLER_TARGET_INVERSE_RED', endpoint)

    def a59_yahoo_label_witness(self):
        rows = {}
        for day in ('2026-08-03', '2026-08-02', '2026-08-05', '1979-12-31'):
            epoch = int(datetime.fromisoformat(day).replace(tzinfo=timezone.utc).timestamp())
            rows[day] = {'date': epoch, 'numerator': 2, 'denominator': 1}
        raw = json.dumps({'chart': {'result': [{'events': {'splits': rows}}]}}).encode()
        with tp.offline_policy() as capability:
            context = self.context(capability=capability)
            accepted = splits.guarded_split_attempt(context.worker('a59-label'), 'http.split_provider.yahoo',
                                                    'TEST', lambda url: raw)
            context.seal()
        self.assertEqual([row['label'] for row in accepted], ['2026-08-03'], 'Yahoo accepted UTC labels')
        self.assertEqual(accepted[0]['source_epoch'], rows['2026-08-03']['date'])
        self.assertEqual(accepted[0]['timestamp'], '2026-08-03T20:00:00+00:00')
        events = inspect_ledger(context.ledger.path)['events']
        evidence, result = events[0]['payload']['http_request'], events[1]['payload']
        self.assertEqual(evidence['period2'], int(context.horizons['1d'].timestamp()), 'Yahoo exact horizon epoch')
        self.assertEqual(dict(parse_qsl(urlsplit(evidence['url']).query))['period2'],
                         str(int(context.horizons['1d'].timestamp())), 'Yahoo exact wire horizon epoch')
        self.assertEqual(evidence['requested_start_date'], '1980-01-01')
        self.assertTrue(evidence['truncated_coverage'])
        self.assertEqual((result['observed']['count'], result['accepted']['count'], result['dropped']), (4, 1, 3))
        self.assertNotEqual(result['observed']['digest'], result['accepted']['digest'])
        self.assertNotEqual(result['raw_response_digest'], result['observed']['digest'])

    def test_a59_split_yahoo_label_evidence_and_inverses(self):
        self.a59_yahoo_label_witness()
        for function, old, new, reason in (
                (splits.normalize_yahoo_events, 'datetime.fromtimestamp(epoch, timezone.utc)',
                 'datetime.fromtimestamp(epoch, __import__("fetch_authority").NY)', 'Yahoo accepted UTC labels'),
                (splits.filter_yahoo_events, 'if window is None:\n                continue',
                 'if window is None:\n                accepted.append(row)\n                continue', 'Yahoo accepted UTC labels'),
                (splits._spec, "('period2', int(end.timestamp()))",
                 "('period2', int(end.timestamp()) + 7 * 86400)", 'Yahoo exact wire horizon epoch')):
            with self.subTest(mutation=reason):
                mutant = _source_inverse(function, old, new)
                with patch.object(splits, function.__name__, mutant):
                    with self.assertRaisesRegex(AssertionError, reason):
                        self.a59_yahoo_label_witness()
                print('A59_YAHOO_LABEL_INVERSE_RED', function.__name__)

    def a59_logical_binding_witness(self):
        producer = 'http.split_provider.sec_tickers'
        with tp.offline_policy() as capability:
            context = self.context(capability=capability)
            retained = splits.split_request(context.worker('a59-logical-binding'), producer,
                                            'TEST', **self.a59_split_options(producer))
            sender = Mock(return_value=self.a59_split_fixture(producer))
            changed = replace(retained.evidence, symbol='WRONG')
            with self.assertRaises(RequestRefused):
                retained.request.execute(lambda effective: sender(), acquire_turn=lambda: 0,
                    governor=context.governors.provider('sec.tickers'),
                    normalizer=splits.decode_split_json, attempt_evidence=changed)
            sender.assert_not_called()
            self.assertEqual(inspect_ledger(context.ledger.path, require_seal=False)['events'], [])

    def test_a59_split_request_provenance_holds(self):
        self.a59_logical_binding_witness()
        mutant = _source_inverse(splits.LogicalRequest._execute,
            'if self._http_binding is None or attempt_evidence != self._http_binding:\n'
            '            raise RequestRefused("split logical request provenance binding changed")',
            'if False:\n            raise RequestRefused("split logical request provenance binding changed")')
        with patch.object(splits.LogicalRequest, '_execute', mutant):
            with self.assertRaisesRegex(AssertionError, 'RequestRefused not raised'):
                self.a59_logical_binding_witness()
        print('A59_LOGICAL_BINDING_INVERSE_RED')
        mutations = ('url', 'headers', 'symbol', 'producer', 'cik')
        for producer, endpoint, _, _ in self._A23B_BINDINGS:
            for mutation in mutations:
                with self.subTest(producer=producer, mutation=mutation), tp.offline_policy() as capability:
                    context = self.context(capability=capability)
                    worker = context.worker('a59-provenance')
                    retained = splits.split_request(worker, producer,
                        'FTNT' if endpoint == 'fortinet.history' else 'TEST', **self.a59_split_options(producer))
                    evidence = retained.evidence
                    edits = {'url': evidence.url + '&unexpected=1',
                             'headers': evidence.headers + (('X-Extra', 'refused'),),
                             'symbol': 'WRONG', 'producer': 'http.stockanalysis.validation',
                             'cik': 123}
                    changed = replace(evidence, **{mutation: edits[mutation]})
                    prior_time = self.clock.value
                    with self.assertRaises(RequestRefused), patch.object(context.ledger, 'decision') as decision:
                        retained.request.execute(Mock(), acquire_turn=Mock(), normalizer=splits.decode_split_json,
                            governor=context.governors.provider(endpoint), attempt_evidence=changed)
                    decision.assert_not_called()
                    self.assertEqual(self.clock.value, prior_time, 'provenance hold before governor turn')
        for bad in (None, '', 'Data Bank SplitCache contact: operator', 'bad\r\ncontact@example.invalid',
                    'é contact@example.invalid', 'x' * 257, 'no-contact'):
            with self.subTest(sec_configuration=repr(bad)):
                with self.assertRaisesRegex(RequestRefused, 'SEC_USER_AGENT'):
                    splits.validate_sec_user_agent(bad)

    def a59_payload_cik_witness(self):
        producer = 'http.split_provider.sec_facts'
        with tp.offline_policy() as capability:
            context = self.context(capability=capability)
            sender = Mock(return_value=b'{"cik":999,"facts":{}}')
            with self.assertRaises(ResponseQuarantined):
                splits.guarded_split_attempt(context.worker('a59-payload-cik'), producer,
                                              'TEST', sender, **self.a59_split_options(producer))
            self.assertEqual(sender.call_count, 1)
            result = inspect_ledger(context.ledger.path, require_seal=False)['events'][1]['payload']
            self.assertEqual(result['outcome'], 'error')

    def test_a59_split_metadata_quarantine_and_identity(self):
        self.a59_payload_cik_witness()
        mutant = _source_inverse(splits.normalize_split_metadata,
            "if payload_cik != expected_cik:", 'if False:')
        with patch.object(splits, 'normalize_split_metadata', mutant):
            with self.assertRaisesRegex(AssertionError, 'ResponseQuarantined not raised'):
                self.a59_payload_cik_witness()
        print('A59_PAYLOAD_CIK_INVERSE_RED')
        bad_rows = (
            ('http.split_provider.yahoo', b'{"chart":{"result":[{"events":{"splits":{"x":{"date":1785715200,"numerator":NaN,"denominator":1}}}}]}}'),
            ('http.split_provider.yahoo', b'{"chart":{"result":[{"events":{"splits":{"x":{"date":1785715200,"numerator":2,"denominator":1},"y":{"date":1785715200,"numerator":3,"denominator":1}}}}]}}'),
            ('http.split_provider.yahoo', b'{"chart":{"result":[],"result":[]}}'),
            ('http.split_provider.sec_tickers', b'{"0":{"ticker":"TEST","cik_str":1},"1":{"ticker":"TEST","cik_str":2}}'),
            ('http.split_provider.sec_tickers', b'{"0":{"ticker":"TEST","cik_str":true}}'),
            ('http.split_provider.sec_facts', b'{"cik":999,"facts":{}}'),
            ('http.split_provider.issuer', b'Unproven issuer history'),
        )
        for producer, raw in bad_rows:
            with self.subTest(producer=producer, digest=hashlib.sha256(raw).hexdigest()), tp.offline_policy() as capability:
                context = self.context(capability=capability)
                sends = []
                with self.assertRaises(ResponseQuarantined) as refused:
                    splits.guarded_split_fetch(context.worker('a59-malformed'), producer,
                        'FTNT' if producer.endswith('issuer') else 'TEST',
                        lambda url: sends.append(url) or raw, attempts=3, **self.a59_split_options(producer))
                self.assertEqual(len(sends), 1, 'quarantined payload must never retry')
                self.assertTrue(refused.exception.quarantine_path)
                events = inspect_ledger(context.ledger.path, require_seal=False)['events']
                self.assertEqual(len(events), 2)
                self.assertEqual(events[1]['payload']['raw_response_digest'], hashlib.sha256(raw).hexdigest())
                quarantine = next(context.ledger.path.parent.glob(context.ledger.path.stem + '.*.quarantine.json'))
                body = json.loads(quarantine.read_bytes())['response']
                self.assertEqual(base64.b64decode(body['raw_response_base64']), raw)
                self.assertEqual(context.ledger.http_evidence_usage()['retained_bytes'], quarantine.stat().st_size)
                self.assertEqual(context.ledger.http_evidence_usage()['reserved_bytes'], 0)

    def a59_three_attempt_witness(self):
        producer = 'http.split_provider.yahoo'
        with tp.offline_policy() as capability:
            context = self.context(capability=capability)
            worker = context.worker('a59-retry-bound')
            retained = splits.split_request(worker, producer, 'TEST')
            sender = Mock(return_value=self.a59_split_fixture(producer))
            for _ in range(3):
                splits.guarded_split_attempt(worker, producer, 'TEST', sender, _logical_request=retained)
            with self.assertRaises(RequestRefused):
                splits.guarded_split_attempt(worker, producer, 'TEST', sender, _logical_request=retained)
            self.assertEqual(sender.call_count, 3)
            self.assertEqual(len(inspect_ledger(context.ledger.path, require_seal=False)['events']), 6)

    def test_a59_split_retry_governor_and_terminals(self):
        self.a59_three_attempt_witness()
        mutant = _source_inverse(splits.guarded_split_attempt,
            'if next(retained._attempts) > 3:', 'if next(retained._attempts) > 4:')
        with patch.object(splits, 'guarded_split_attempt', mutant):
            with self.assertRaisesRegex(AssertionError, 'RequestRefused not raised'):
                self.a59_three_attempt_witness()
        print('A59_THREE_ATTEMPT_BOUND_INVERSE_RED')
        for producer, endpoint, _, _ in self._A23B_BINDINGS:
            with self.subTest(producer=producer, case='transient-three'), tp.offline_policy() as capability:
                context = self.context(capability=capability)
                sends = []
                def send(url):
                    sends.append(url)
                    if len(sends) < 3:
                        raise TimeoutError('finite attempt timeout')
                    return self.a59_split_fixture(producer)
                accepted = splits.guarded_split_fetch(context.worker('a59-retry'), producer,
                    'FTNT' if endpoint == 'fortinet.history' else 'TEST', send, attempts=3, **self.a59_split_options(producer))
                self.assertTrue(accepted)
                context.seal()
                events = inspect_ledger(context.ledger.path)['events'][:-1]
                self.assertEqual(len(events), 6)
                self.assertEqual(len({event['logical_id'] for event in events}), 1)
                self.assertEqual(len({event['attempt_id'] for event in events}), 3)
                self.assertEqual(len(set(sends)), 1)
                self.assertTrue(all(event['payload']['pacer_wait_seconds'] >= 1 for event in events[2::2]))
        self.assertIs(self.governors.provider('sec.tickers'), self.governors.provider('sec.companyfacts'))
        for code, producer, retry in ((404, 'http.split_provider.yahoo', False),
                                     (410, 'http.split_provider.yahoo', False),
                                     (429, 'http.split_provider.yahoo', True),
                                     (503, 'http.split_provider.yahoo', True),
                                     (403, 'http.split_provider.sec_tickers', True)):
            with self.subTest(status=code), tp.offline_policy() as capability:
                context = self.context(capability=capability)
                opens = []
                def send(url):
                    opens.append(url)
                    if len(opens) == 1:
                        raise HTTPError(url, code, 'finite fixture', {'Retry-After': '2'}, None)
                    return self.a59_split_fixture(producer)
                try:
                    splits.guarded_split_fetch(context.worker('a59-status'), producer, 'TEST', send,
                        **self.a59_split_options(producer))
                except HTTPError:
                    self.assertFalse(retry)
                else:
                    self.assertTrue(retry)
                self.assertEqual(len(opens), 2 if retry else 1)
        for fault in (LedgerError('durability sentinel'), AuthorityError('busy DNS slot'),
                      RequestCancelled('cancel sentinel')):
            with self.subTest(fault=type(fault).__name__), tp.offline_policy() as capability:
                context = self.context(capability=capability)
                sends = Mock(side_effect=fault)
                with self.assertRaises(type(fault)):
                    splits.guarded_split_fetch(context.worker('a59-terminal'), 'http.split_provider.yahoo',
                                              'TEST', sends, attempts=3)
                self.assertEqual(sends.call_count, 1)

    def a59_evidence_budget_witness(self):
        with tp.offline_policy() as capability:
            context = self.context(capability=capability)
            sender = Mock(return_value=self.a59_split_fixture('http.split_provider.yahoo'))
            amount = 4 * (8 * 1024 * 1024 + 1) + 65536
            with patch.object(ledger_module, 'HTTP_EVIDENCE_BUDGET', amount - 1):
                with self.assertRaisesRegex(LedgerError, 'budget exhausted before provider open'):
                    splits.guarded_split_attempt(context.worker('a59-budget'), 'http.split_provider.yahoo', 'TEST', sender)
            sender.assert_not_called()
            self.assertEqual(inspect_ledger(context.ledger.path, require_seal=False)['events'], [])

    def test_a59_split_evidence_budget_and_disposal(self):
        self.a59_evidence_budget_witness()
        mutant = _source_inverse(FetchLedger.reserve_http_evidence,
            'if self._http_reserved_bytes + self._http_retained_bytes + amount > HTTP_EVIDENCE_BUDGET:',
            'if False:')
        with patch.object(FetchLedger, 'reserve_http_evidence', mutant):
            with self.assertRaisesRegex(AssertionError, 'LedgerError not raised'):
                self.a59_evidence_budget_witness()
        print('A59_EVIDENCE_BUDGET_INVERSE_RED')
        with tp.offline_policy() as capability:
            context = self.context(capability=capability)
            reservation = context.ledger.reserve_http_evidence('a59-owned-held', 'sec.companyfacts')
            amount = 4 * (16 * 1024 * 1024 + 1) + 65536
            self.assertEqual(context.ledger.http_evidence_usage()['reserved_bytes'], amount)
            self.assertLessEqual(amount, ledger_module.HTTP_EVIDENCE_BUDGET)
            self.assertGreater(2 * amount, ledger_module.HTTP_EVIDENCE_BUDGET)
            sender = Mock(return_value=self.a59_split_fixture('http.split_provider.sec_facts'))
            with self.assertRaisesRegex(LedgerError, 'budget exhausted before provider open'):
                splits.guarded_split_attempt(context.worker('a59-second-reservation'),
                    'http.split_provider.sec_facts', 'TEST', sender,
                    **self.a59_split_options('http.split_provider.sec_facts'))
            sender.assert_not_called()
            self.assertEqual(self.clock.value, 0, 'reservation refuses before provider turn')
            self.assertEqual(inspect_ledger(context.ledger.path, require_seal=False)['events'], [])
            context.ledger.finish_http_evidence(reservation)
            self.assertEqual(context.ledger.http_evidence_usage()['reserved_bytes'], 0)
            accepted = splits.guarded_split_attempt(context.worker('a59-after-release'),
                'http.split_provider.sec_facts', 'TEST', sender,
                **self.a59_split_options('http.split_provider.sec_facts'))
            self.assertEqual(accepted['cik'], 789019)
            self.assertEqual(sender.call_count, 1)
            self.assertEqual(context.ledger.http_evidence_usage()['reserved_bytes'], 0)
            context.seal()
        for producer, endpoint, _, _ in self._A23B_BINDINGS:
            with self.subTest(endpoint=endpoint, case='cap-plus-one'), tp.offline_policy() as capability:
                context = self.context(capability=capability)
                caps = dict(ledger_module.HTTP_BODY_CAPS, **{endpoint: 16})
                responses = []
                def wire_open(_opener, wire, *, timeout):
                    response = io.BytesIO(b'x' * 100)
                    responses.append(response)
                    return response
                capture = {}
                with (patch.object(ledger_module, 'HTTP_BODY_CAPS', caps),
                      patch.object(splits, 'HTTP_BODY_CAPS', caps),
                      patch.object(urllib.request.OpenerDirector, 'open', wire_open)):
                    with self.assertRaises(ResponseQuarantined):
                        splits.guarded_split_attempt(context.worker('a59-cap'), producer,
                            'FTNT' if endpoint == 'fortinet.history' else 'TEST',
                            _capture=capture, **self.a59_split_options(producer))
                self.assertEqual(capture['observed_bytes'], 17)
                self.assertTrue(responses[0].closed)
                self.assertEqual(context.ledger.http_evidence_usage()['reserved_bytes'], 0)
        class BrokenClose(io.BytesIO):
            def close(self):
                raise OSError('owned cleanup failure')
        response = BrokenClose(self.a59_split_fixture('http.split_provider.yahoo'))
        self.addCleanup(io.BytesIO.close, response)
        with tp.offline_policy() as capability:
            context = self.context(capability=capability)
            with patch.object(urllib.request.OpenerDirector, 'open', return_value=response):
                with self.assertRaisesRegex(LedgerError, 'cleanup failed'):
                    splits.guarded_split_fetch(context.worker('a59-cleanup'), 'http.split_provider.yahoo', 'TEST', attempts=3)


    def a58_metadata_exchange(self, producer, payload, *, quarantine=False):
        """Real executor/result durability; finite bytes, never a provider read."""
        raw = payload if type(payload) is bytes else json.dumps(payload, allow_nan=False).encode()
        with tp.offline_policy() as capability:
            context = self.context(capability=capability)
            sender = Mock(return_value=raw)
            capture = {}
            accepted = None
            symbol = 'FTNT' if producer.endswith('issuer') else 'TEST'
            if quarantine:
                with self.assertRaises(ResponseQuarantined) as refused:
                    splits.guarded_split_attempt(context.worker('a58-metadata'), producer,
                        symbol, sender, _capture=capture, **self.a59_split_options(producer))
                retained = json.loads((context.ledger.path.parent / refused.exception.quarantine_path).read_bytes())
                self.assertEqual(base64.b64decode(retained['response']['raw_response_base64']), raw)
            else:
                accepted = splits.guarded_split_attempt(context.worker('a58-metadata'), producer,
                    symbol, sender, _capture=capture, **self.a59_split_options(producer))
            self.assertEqual(sender.call_count, 1)
            self.assertEqual(capture, {'raw_digest': hashlib.sha256(raw).hexdigest(),
                                       'observed_bytes': len(raw)})
            self.assertEqual(context.ledger.http_evidence_usage()['reserved_bytes'], 0)
            context.seal()
        events = inspect_ledger(context.ledger.path)['events']
        self.assertEqual(len(events), 3)
        result = events[1]['payload']
        self.assertEqual(result['raw_response_digest'], hashlib.sha256(raw).hexdigest())
        self.assertEqual(result['outcome'] == 'error', quarantine)
        if not quarantine and not producer.endswith('yahoo'):
            self.assertEqual(result['response_digest'], digest_value(accepted))
            self.assertNotEqual(result['response_digest'], result['raw_response_digest'])
        for name in self.a59_split_options(producer):
            if name == 'sec_user_agent':
                self.assertNotIn(self.a59_split_options(producer)[name].encode(), context.ledger.path.read_bytes())
        return accepted, result

    def a58_row_accounting(self, result, observed, accepted, reasons, normalized=0,
                           colliding_identity_count=0, **identity):
        rows = result['metadata_rows']
        if 'payload_identity_normalized' in identity:
            identity['colliding_identity_count'] = colliding_identity_count
        self.assertEqual(rows, dict(observed_count=observed, accepted_count=accepted,
            dropped_count=observed - accepted, dropped_reasons=reasons,
            normalized_count=normalized, **identity), 'A58 exact metadata row accounting')
        self.assertEqual(rows['observed_count'], rows['accepted_count'] + rows['dropped_count'])
        self.assertEqual(sum(rows['dropped_reasons'].values()), rows['dropped_count'])

    def a58_facts_payload(self, rows, cik=789019):
        return {'cik': cik, 'facts': {'us-gaap': {
            'StockholdersEquityNoteStockSplitConversionRatio1': {'units': {'pure': rows}}}}}

    def test_a58_split_metadata_map_row_policy(self):
        producer = 'http.split_provider.sec_tickers'
        good = {'ticker': 'TEST', 'cik_str': 789019}
        normalizations = (
            ('lowercase', {'ticker': 'brk-b', 'cik_str': 1067983}, 'BRK-B', 1067983),
            ('ascii-surround', {'ticker': ' \tbrk.b\r\n', 'cik_str': 1067983}, 'BRK.B', 1067983),
            ('padded-cik', {'ticker': 'BRK-B', 'cik_str': '0001067983'}, 'BRK-B', 1067983),
            ('decimal-cik', {'ticker': 'BRK.B', 'cik_str': '1067983'}, 'BRK.B', 1067983),
        )
        for name, row, symbol, cik in normalizations:
            with self.subTest(rule=name):
                accepted, result = self.a58_metadata_exchange(producer, {'0': good, '1': row})
                self.assertEqual(accepted, {'TEST': 789019, symbol: cik}, 'A58 normalized map peers')
                self.a58_row_accounting(result, 2, 2, {}, normalized=1)
        drops = (
            ('non-object', None, 'non_object'), ('list-row', [], 'non_object'),
            ('missing-ticker', {'cik_str': 1}, 'invalid_ticker'),
            ('bool-ticker', {'ticker': True, 'cik_str': 1}, 'invalid_ticker'),
            ('embedded-space', {'ticker': 'BRK B', 'cik_str': 1}, 'invalid_ticker'),
            ('empty-ticker', {'ticker': ' \t', 'cik_str': 1}, 'invalid_ticker'),
            ('slash-ticker', {'ticker': 'BAD/ONE', 'cik_str': 1}, 'invalid_ticker'),
            ('long-ticker', {'ticker': 'A' * 31, 'cik_str': 1}, 'invalid_ticker'),
            ('unicode-ticker', {'ticker': 'É', 'cik_str': 1}, 'invalid_ticker'),
            ('unicode-expansion', {'ticker': 'ß', 'cik_str': 1}, 'invalid_ticker'),
            ('unicode-ligature', {'ticker': 'ﬃ', 'cik_str': 1}, 'invalid_ticker'),
            *[(name, {'ticker': 'OTHER', 'cik_str': value}, 'invalid_cik')
              for name, value in (('missing-cik', None), ('bool-cik', True), ('float-cik', 1.0),
                                  ('zero-cik', 0), ('negative-cik', -1), ('large-cik', 10000000000),
                                  ('long-text-cik', '00000000001'), ('signed-cik', '+1'),
                                  ('space-cik', ' 1'), ('unicode-cik', '١'), ('text-cik', 'no'))],
            ('duplicate-identity', {'ticker': 'test', 'cik_str': '0000789019'}, 'duplicate_identity'),
        )
        for name, row, reason in drops:
            with self.subTest(rule=name):
                accepted, result = self.a58_metadata_exchange(producer, {'0': good, '1': row})
                self.assertEqual(accepted, {'TEST': 789019}, 'A58 preserved map peer')
                self.a58_row_accounting(result, 2, 1, {reason: 1})
        for name, payload in (('empty', {}), ('all-dropped', {'0': None}),
                              ('outer-list', []), ('conflict', {'0': good,
                               '1': {'ticker': 'test', 'cik_str': 1}})):
            with self.subTest(rule=name):
                _, result = self.a58_metadata_exchange(producer, payload, quarantine=True)
                self.assertNotIn('metadata_rows', result)
        with patch.object(splits, '_MAX_EVENTS', 1):
            self.a58_metadata_exchange(producer, {'0': good, '1': good}, quarantine=True)

    def test_a58_split_companyfacts_row_policy(self):
        producer = 'http.split_provider.sec_facts'
        good = {'val': 2, 'end': '2026-08-03', 'filed': '2026-08-04',
                'form': '10-Q', 'accn': '0000789019-26-000001'}
        bad_rows = (
            ('non-object', None, 'non_object'), ('list-row', [], 'non_object'),
            *[(name, {**good, 'val': value}, 'invalid_ratio') for name, value in (
                ('zero', 0), ('negative', -2), ('text-ratio', '2'), ('bool-ratio', True),
                ('null-ratio', None), ('unrepresentable-ratio', 10 ** 400))],
            ('missing-val', {k: v for k, v in good.items() if k != 'val'}, 'invalid_ratio'),
            *[(name, {**good, 'end': value}, 'invalid_end') for name, value in (
                ('missing-end', None), ('time-end', '2026-08-03T00:00:00'),
                ('compact-end', '20260803'), ('bad-end', '2026-02-30'), ('number-end', 20260803))],
            *[(name, {**good, 'filed': value}, 'invalid_filed') for name, value in (
                ('time-filed', '2026-08-04T00:00:00'), ('compact-filed', '20260804'),
                ('bad-filed', '2026-02-30'), ('bool-filed', True))],
            *[(name, {**good, 'form': value}, 'invalid_form') for name, value in (
                ('long-form', 'F' * 17), ('unicode-form', 'é'), ('control-form', '10-Q\n'), ('number-form', 10))],
            *[(name, {**good, 'accn': value}, 'invalid_accn') for name, value in (
                ('long-accn', 'A' * 65), ('unicode-accn', 'é'), ('control-accn', 'id\t'), ('bool-accn', True))],
        )
        for name, bad, reason in bad_rows:
            with self.subTest(rule=name):
                accepted, result = self.a58_metadata_exchange(producer, self.a58_facts_payload([good, bad]))
                self.assertEqual(accepted['cik'], 789019)
                self.assertEqual(len(accepted['split_facts']), 1, 'A58 preserved fact peer')
                fact = accepted['split_facts'][0]
                self.assertEqual((fact['ratio'], fact['fact_date'], fact['accession']),
                                 (2.0, '2026-08-03', good['accn']))
                self.assertEqual(accepted['records'][fact['source_id']]['ratio'], 2.0)
                self.a58_row_accounting(result, 2, 1, {reason: 1}, payload_identity_normalized=False)
        for name, row in (('absent-optionals', {'val': 2, 'end': '2026-08-03'}),
                          ('null-optionals', {'val': 2, 'end': '2026-08-03', 'filed': None,
                                             'form': None, 'accn': None}),
                          ('exact-bounds', {**good, 'form': 'F' * 16, 'accn': 'A' * 64})):
            with self.subTest(rule=name):
                accepted, result = self.a58_metadata_exchange(producer, self.a58_facts_payload([row], '0000789019'))
                self.assertEqual(len(accepted['split_facts']), 1)
                self.a58_row_accounting(result, 1, 1, {}, payload_identity_normalized=True)
        duplicate = {**good, 'filed': '2026-08-05', 'form': '10-K', 'accn': ' ' + good['accn'] + ' '}
        accepted, result = self.a58_metadata_exchange(producer, self.a58_facts_payload([good, duplicate]))
        self.assertEqual(accepted['split_facts'][0]['filed'], good['filed'])
        self.a58_row_accounting(result, 2, 1, {'duplicate_fact': 1}, payload_identity_normalized=False)
        for name, rows in (('ratio-conflict', [good, {**good, 'val': 3}]),
                           ('date-conflict', [good, {**good, 'end': '2026-08-04'}])):
            with self.subTest(rule=name):
                accepted, result = self.a58_metadata_exchange(producer, self.a58_facts_payload(rows))
                self.assertEqual((accepted['split_facts'], accepted['records']), ([], {}))
                self.a58_row_accounting(result, 2, 0, {'source_identity_collision': 2},
                                        payload_identity_normalized=False,
                                        colliding_identity_count=1)
        for value in (None, True, 789019.0, 0, -1, 999, '0000000999', '00000789019', '+789019', '٧٨٩٠١٩'):
            with self.subTest(payload_cik=repr(value)):
                self.a58_metadata_exchange(producer, self.a58_facts_payload([good], value), quarantine=True)
        with patch.object(splits, '_MAX_EVENTS', 1):
            self.a58_metadata_exchange(producer, self.a58_facts_payload([good, good]), quarantine=True)

    def test_a58_split_optional_shapes_and_strict_epochs(self):
        yahoo = 'http.split_provider.yahoo'
        for name, container in (('events-absent', {}), ('events-null', {'events': None}),
                                 ('splits-absent', {'events': {}}), ('splits-null', {'events': {'splits': None}})):
            with self.subTest(rule=name):
                accepted, result = self.a58_metadata_exchange(yahoo, {'chart': {'result': [container]}})
                self.assertEqual(accepted, [])
                self.assertEqual(result['outcome'], 'empty')
                self.assertEqual((result['observed']['count'], result['accepted']['count'], result['dropped']), (0, 0, 0))
        for epoch in (None, True, 1785715200.0, '1785715200'):
            with self.subTest(epoch=repr(epoch)):
                self.a58_metadata_exchange(yahoo, {'chart': {'result': [{'events': {'splits': {
                    'x': {'date': epoch, 'numerator': 2, 'denominator': 1}}}}]}}, quarantine=True)
        for container in ({'events': []}, {'events': {'splits': []}}, {'events': {'splits': {'x': None}}}):
            with self.subTest(shape=repr(container)):
                self.a58_metadata_exchange(yahoo, {'chart': {'result': [container]}}, quarantine=True)
        facts = 'http.split_provider.sec_facts'
        for depth in range(5):
            for value in ('absent', None, [], 'wrong'):
                payload = {'cik': 789019}
                cursor = payload
                keys = ('facts', 'us-gaap', 'StockholdersEquityNoteStockSplitConversionRatio1', 'units', 'pure')
                for key in keys[:depth]:
                    cursor[key] = {}
                    cursor = cursor[key]
                if value != 'absent':
                    cursor[keys[depth]] = value
                invalid = value in ([], 'wrong') and not (depth == 4 and value == [])
                with self.subTest(container=keys[depth], value=repr(value)):
                    accepted, result = self.a58_metadata_exchange(facts, payload, quarantine=invalid)
                    if not invalid:
                        self.assertEqual(accepted, {'cik': 789019, 'split_facts': [], 'records': {}})
                        self.a58_row_accounting(result, 0, 0, {}, payload_identity_normalized=False)
        for raw in (b'{"cik":789019,"cik":789019}', b'{"cik":789019,"facts":{"us-gaap":NaN}}',
                    b'{"cik":789019,"facts":{"us-gaap":1e999}}'):
            with self.subTest(strict_json_digest=hashlib.sha256(raw).hexdigest()):
                self.a58_metadata_exchange(facts, raw, quarantine=True)


    def a58_injected_hold_witness(self, producer):
        context = self.context()
        sender = Mock(return_value=self.a59_split_fixture(producer))
        with patch.object(urllib.request.OpenerDirector, 'open') as opener:
            with self.assertRaisesRegex(RequestRefused, '^split injected transport requires offline policy$'):
                splits.guarded_split_attempt(context.worker('a58-injected-hold'), producer,
                    'FTNT' if producer.endswith('issuer') else 'TEST', sender,
                    **self.a59_split_options(producer))
            opener.assert_not_called()
        sender.assert_not_called()
        self.assertEqual(inspect_ledger(context.ledger.path, require_seal=False)['events'], [])

    def test_a58_split_injected_policy_guard_and_inverse(self):
        mutant = _source_inverse(splits.guarded_split_attempt,
            'if ((send is not None or context.test_capability is not None)\n'
            '            and not is_live_capability(context.test_capability)):', 'if False:')
        for producer, _, _, _ in self._A23B_BINDINGS:
            with self.subTest(producer=producer):
                self.a58_injected_hold_witness(producer)
                with patch.object(splits, 'guarded_split_attempt', mutant):
                    with self.assertRaisesRegex(AssertionError, 'does not match'):
                        self.a58_injected_hold_witness(producer)
                print('A58_INJECTED_POLICY_INVERSE_RED', producer)

    def a58_creation_witness(self, producer, change):
        with tp.offline_policy() as capability:
            context = self.context(capability=capability)
            worker = context.worker('a58-creation')
            retained = splits.split_request(worker, producer,
                'FTNT' if producer.endswith('issuer') else 'TEST', **self.a59_split_options(producer))
            binding = replace(retained.evidence, **change)
            with self.assertRaisesRegex(RequestRefused, '^split attempt provenance changed$'):
                worker.request(producer, retained.request.envelope.wire(), _http_binding=binding)
            self.assertEqual(inspect_ledger(context.ledger.path, require_seal=False)['events'], [])
            self.assertEqual(self.clock.value, 0)

    def a58_shape_witness(self, producer, symbol='TEST', reason='', **options):
        with tp.offline_policy() as capability:
            context = self.context(capability=capability)
            with self.assertRaisesRegex(RequestRefused, reason), patch.object(urllib.request.OpenerDirector, 'open') as opener:
                splits.split_request(context.worker('a58-shape'), producer, symbol, **options)
            opener.assert_not_called()
            self.assertEqual(inspect_ledger(context.ledger.path, require_seal=False)['events'], [])
            self.assertEqual(self.clock.value, 0)

    def test_a58_split_creation_and_shape_guards(self):
        mutant = _source_inverse(splits.verified_split_attempt_evidence,
            'if envelope != expected_envelope or evidence != expected:', 'if False:')
        for producer, _, _, _ in self._A23B_BINDINGS:
            with tp.offline_policy() as capability:
                context = self.context(capability=capability)
                worker = context.worker('a58-creation-types')
                retained = splits.split_request(worker, producer,
                    'FTNT' if producer.endswith('issuer') else 'TEST', **self.a59_split_options(producer))
                with self.subTest(producer=producer, malformed_shape='fourth-header'):
                    malformed = replace(retained.evidence,
                        headers=retained.evidence.headers + (('X-Extra', 'forged'),))
                    with self.assertRaisesRegex(RequestRefused,
                            '^split attempt provenance changed: fields are not exact$'):
                        worker.request(producer, retained.request.envelope.wire(), _http_binding=malformed)
                    self.assertEqual(inspect_ledger(context.ledger.path, require_seal=False)['events'], [])
                    self.assertEqual(self.clock.value, 0)
                    print('A66_MALFORMED_FOURTH_HEADER_REFUSED', producer)
                for bad in ({}, {'url': retained.evidence.url}, 'unsealed', object()):
                    with self.subTest(producer=producer, binding_type=type(bad).__name__):
                        with self.assertRaisesRegex(RequestRefused, 'provenance is missing'):
                            worker.request(producer, retained.request.envelope.wire(), _http_binding=bad)
                for shape in ('url', 'headers'):
                    change = ({'url': retained.evidence.url + '?forged=1'} if shape == 'url' else
                              {'headers': tuple((key, 'gzip' if key == 'Accept-Encoding' else value)
                                                for key, value in retained.evidence.headers)})
                    with self.subTest(producer=producer, creation_shape=shape):
                        self.a58_creation_witness(producer, change)
                        with patch.object(splits, 'verified_split_attempt_evidence', mutant):
                            with self.assertRaisesRegex(AssertionError, 'RequestRefused not raised'):
                                self.a58_creation_witness(producer, change)
                        print('A58_CREATION_BINDING_INVERSE_RED', producer, shape)
        shapes = (
            ('agent', 'http.split_provider.sec_tickers', 'TEST', 'SEC_USER_AGENT', {'sec_user_agent': 'no-contact'},
             'agent = validate_sec_user_agent(sec_agent)', 'agent = sec_agent'),
            ('wrong-cik', 'http.split_provider.yahoo', 'TEST', 'CIK belongs only', {'cik': 1},
             'if endpoint != "sec.companyfacts" and cik is not None:', 'if False:'),
            ('wrong-header', 'http.split_provider.yahoo', 'TEST', 'SEC header belongs only',
             {'sec_user_agent': 'fixture fixture@example.invalid'}, 'if sec_agent is not None:', 'if False:'),
            ('wrong-issuer', 'http.split_provider.issuer', 'TEST', 'FTNT-only', {},
             'if symbol != "FTNT":', 'if False:'),
        )
        for name, producer, symbol, reason, options, old, new in shapes:
            with self.subTest(guard=name):
                self.a58_shape_witness(producer, symbol, reason, **options)
                inverse = _source_inverse(splits._spec, old, new)
                with patch.object(splits, '_spec', inverse):
                    with self.assertRaisesRegex(AssertionError, 'RequestRefused not raised'):
                        self.a58_shape_witness(producer, symbol, reason, **options)
                print('A58_REQUEST_SHAPE_INVERSE_RED', name)
        for producer in ('http.split_provider.sec_tickers', 'http.split_provider.sec_facts'):
            for agent in (None, '', 'contact-free', 'x\ncontact@example.invalid', 'é contact@example.invalid',
                          'x' * 257, True):
                with self.subTest(producer=producer, bad_agent=repr(agent)):
                    options = dict(self.a59_split_options(producer), sec_user_agent=agent)
                    self.a58_shape_witness(producer, reason='SEC_USER_AGENT', **options)
        for producer, endpoint, _, _ in self._A23B_BINDINGS:
            if endpoint != 'sec.companyfacts':
                with self.subTest(producer=producer, wrong_endpoint_cik=True):
                    self.a58_shape_witness(producer, 'FTNT' if endpoint == 'fortinet.history' else 'TEST',
                        'CIK belongs only', **dict(self.a59_split_options(producer), cik=1))
            for symbol in (None, True, '', 'test', 'A' * 31, 'BAD/ONE', 'BAD?ONE', 'BAD ONE'):
                with self.subTest(producer=producer, symbol=repr(symbol)):
                    self.a58_shape_witness(producer, symbol, 'not canonical', **self.a59_split_options(producer))
        for cik in (None, True, '789019', 789019.0, 0, -1, 10000000000):
            with self.subTest(request_cik=repr(cik)):
                self.a58_shape_witness('http.split_provider.sec_facts', reason='ten-digit identity',
                    sec_user_agent='fixture fixture@example.invalid', cik=cik)
        with tp.offline_policy() as capability:
            context = self.context(capability=capability)
            for symbol in ('BRK.B', 'BRK-B'):
                retained = splits.split_request(context.worker('a58-canonical'), 'http.split_provider.yahoo', symbol)
                self.assertEqual(urlsplit(retained.evidence.url).path, '/v8/finance/chart/' + symbol)

    def a58_boundary_witness(self, producer, endpoint, overflow):
        cap = 1008  # Seven-byte chunk boundary: a loop stopping at cap loses the overflow byte.
        prefix = self.a59_split_fixture(producer)
        self.assertLess(len(prefix), cap)
        raw = prefix + b' ' * (cap + int(overflow) - len(prefix))
        class Chunks(io.BytesIO):
            def read1(response, size):
                response.read_sizes.append(size)
                return io.BytesIO.read(response, min(size, 7))
        response = Chunks(raw)
        response.read_sizes = []
        self.addCleanup(response.close)
        capture = {}
        caps = dict(ledger_module.HTTP_BODY_CAPS, **{endpoint: cap})
        with tp.offline_policy() as capability:
            context = self.context(capability=capability)
            with (patch.object(ledger_module, 'HTTP_BODY_CAPS', caps),
                  patch.object(splits, 'HTTP_BODY_CAPS', caps),
                  patch.object(urllib.request.OpenerDirector, 'open', return_value=response) as opener):
                if overflow:
                    with self.assertRaises(ResponseQuarantined):
                        splits.guarded_split_attempt(context.worker('a58-boundary'), producer,
                            'FTNT' if producer.endswith('issuer') else 'TEST', _capture=capture,
                            **self.a59_split_options(producer))
                else:
                    accepted = splits.guarded_split_attempt(context.worker('a58-boundary'), producer,
                        'FTNT' if producer.endswith('issuer') else 'TEST', _capture=capture,
                        **self.a59_split_options(producer))
                    self.assertTrue(accepted)
                self.assertEqual(opener.call_count, 1)
            context.seal()
        self.assertTrue(response.closed, 'A58 owned boundary response closed')
        self.assertEqual(capture, {'raw_digest': hashlib.sha256(raw).hexdigest(), 'observed_bytes': len(raw)})
        self.assertEqual(len(response.read_sizes), cap // 7 + 1)
        self.assertEqual(response.read_sizes[-1], 1)
        result = inspect_ledger(context.ledger.path)['events'][1]['payload']
        self.assertEqual(result['raw_response_digest'], hashlib.sha256(raw).hexdigest())
        self.assertEqual(result['outcome'] == 'error', overflow)

    def test_a58_split_parseable_cap_boundaries_and_inverses(self):
        loop = _source_inverse(splits.guarded_split_attempt, 'while len(body) <= cap:', 'while len(body) < cap:')
        for producer, endpoint, _, _ in self._A23B_BINDINGS:
            for overflow in (False, True):
                with self.subTest(endpoint=endpoint, overflow=overflow):
                    self.a58_boundary_witness(producer, endpoint, overflow)
            with patch.object(splits, 'guarded_split_attempt', loop):
                with self.assertRaisesRegex(AssertionError, 'ResponseQuarantined not raised'):
                    self.a58_boundary_witness(producer, endpoint, True)
            print('A58_CAP_PLUS_ONE_READ_INVERSE_RED', endpoint)
            function = splits.decode_split_json if endpoint == 'yahoo.splits' else splits.normalize_split_metadata
            old = ('not raw or len(raw) > max_bytes' if endpoint == 'yahoo.splits' else
                   'not raw or len(raw) > HTTP_BODY_CAPS[envelope.endpoint]')
            inverse = _source_inverse(function, old, 'not raw')
            with patch.object(splits, function.__name__, inverse):
                with self.assertRaisesRegex(AssertionError, 'ResponseQuarantined not raised'):
                    self.a58_boundary_witness(producer, endpoint, True)
            print('A58_NORMALIZER_CAP_INVERSE_RED', endpoint)


    def a58_retry_witness(self, producer, fault_kind, retry, *, persistent=False, attempts=None):
        raw = self.a59_split_fixture(producer)
        opens, owned_errors, responses = [], [], []
        with tp.offline_policy() as capability:
            context = self.context(capability=capability)
            def wire_open(_opener, wire, *, timeout):
                opens.append(wire.full_url)
                if len(opens) == 1 or persistent:
                    if type(fault_kind) is int:
                        body = io.BytesIO(b'finite error response')
                        self.addCleanup(body.close)
                        error = HTTPError(wire.full_url, fault_kind, 'finite status', Message(), body)
                        owned_errors.append((body, error))
                    elif fault_kind == 'url-timeout':
                        error = URLError(TimeoutError('finite wrapped timeout'))
                    elif fault_kind == 'url-reset':
                        error = URLError(ConnectionResetError('finite wrapped reset'))
                    elif fault_kind == 'url-text':
                        error = URLError('finite non-transport reason')
                    else:
                        error = TimeoutError('finite persistent timeout')
                    raise error
                response = io.BytesIO(raw)
                responses.append(response)
                self.addCleanup(response.close)
                return response
            caught, accepted = None, None
            options = self.a59_split_options(producer)
            if attempts is not None:
                options['attempts'] = attempts
            with patch.object(urllib.request.OpenerDirector, 'open', wire_open):
                try:
                    accepted = splits.guarded_split_fetch(context.worker('a58-retry'), producer,
                        'FTNT' if producer.endswith('issuer') else 'TEST', **options)
                except (HTTPError, URLError, TimeoutError) as error:
                    caught = error
            expected = (attempts or 2) if persistent else 2 if retry else 1
            self.assertEqual(len(opens), expected, 'A58 exact retry physical attempts')
            self.assertEqual(caught is not None, persistent or not retry, 'A58 retry final disposition')
            if caught is None:
                self.assertTrue(accepted)
            self.assertTrue(all(body.closed for body, _ in owned_errors), 'A58 owned HTTP error closed')
            self.assertTrue(all(response.closed for response in responses), 'A58 owned success response closed')
            self.assertEqual(context.ledger.http_evidence_usage()['reserved_bytes'], 0)
            context.seal()
        events = inspect_ledger(context.ledger.path)['events'][:-1]
        self.assertEqual(len(events), expected * 2)
        self.assertEqual([event['event'] for event in events], ['decision', 'result'] * expected)
        self.assertEqual(len({event['logical_id'] for event in events}), 1)
        self.assertEqual(len({event['attempt_id'] for event in events}), expected)
        self.assertEqual(len(set(opens)), 1)
        for index in range(expected):
            self.assertEqual(events[2 * index]['attempt_id'], events[2 * index + 1]['attempt_id'])
            self.assertEqual(events[2 * index + 1]['payload']['outcome'],
                ('timeout' if fault_kind == 'timeout' else 'error')
                if persistent or index == 0 else 'returned')

    def test_a58_split_default_wire_retry_matrix(self):
        for producer, endpoint, _, _ in self._A23B_BINDINGS:
            for status in (408, 429, *range(500, 600), 400, 401, 403, 404, 410, 425):
                retry = status in (408, 429) or 500 <= status <= 599 or (status == 403 and endpoint.startswith('sec.'))
                with self.subTest(endpoint=endpoint, status=status):
                    self.a58_retry_witness(producer, status, retry)
            for fault, retry in (('url-timeout', True), ('url-reset', True), ('url-text', False)):
                with self.subTest(endpoint=endpoint, wrapped_fault=fault):
                    self.a58_retry_witness(producer, fault, retry)
            for attempts in (None, 3):
                with self.subTest(endpoint=endpoint, exhausted_cap=attempts):
                    self.a58_retry_witness(producer, 'timeout', True, persistent=True, attempts=attempts)
        for attempts in (0, 4, True, 2.0, '2'):
            with self.subTest(invalid_cap=repr(attempts)), tp.offline_policy() as capability:
                context = self.context(capability=capability)
                with patch.object(urllib.request.OpenerDirector, 'open') as opener:
                    with self.assertRaisesRegex(RequestRefused, 'retry cap is invalid'):
                        splits.guarded_split_fetch(context.worker('a58-bad-cap'),
                            'http.split_provider.yahoo', 'TEST', attempts=attempts)
                    opener.assert_not_called()
                self.assertEqual(inspect_ledger(context.ledger.path, require_seal=False)['events'], [])

    def test_a58_split_retry_and_error_close_reached_inverses(self):
        yahoo, sec = 'http.split_provider.yahoo', 'http.split_provider.sec_facts'
        inverses = (
            ('408', splits.guarded_split_fetch, 'error.code in (408, 429)', 'error.code in (429,)', yahoo, 408, True, False),
            ('all-5xx', splits.guarded_split_fetch, '500 <= error.code <= 599', 'error.code == 503', yahoo, 501, True, False),
            ('SEC-only-403', splits.guarded_split_fetch, "retained.request.envelope.endpoint.startswith('sec.') and error.code == 403",
             'error.code == 403', yahoo, 403, False, False),
            ('wrapped-transport', splits.guarded_split_fetch, 'retry = isinstance(error.reason, (TimeoutError, OSError))',
             'retry = False', sec, 'url-reset', True, False),
            ('default-two', splits.guarded_split_fetch, 'attempts=2, **options', 'attempts=3, **options', yahoo, 'timeout', True, True),
            ('HTTP-error-close', splits.guarded_split_attempt, 'response = error', 'response = None', yahoo, 503, True, False),
        )
        for name, function, old, new, producer, fault, retry, persistent in inverses:
            with self.subTest(inverse=name):
                self.a58_retry_witness(producer, fault, retry, persistent=persistent)
                inverse = _source_inverse(function, old, new)
                with patch.object(splits, function.__name__, inverse):
                    with self.assertRaisesRegex(AssertionError,
                            'A58 exact retry physical attempts|A58 owned HTTP error closed'):
                        self.a58_retry_witness(producer, fault, retry, persistent=persistent)
                print('A58_RETRY_CLOSE_INVERSE_RED', name)

    def a58_body_cancel_witness(self, producer):
        cancel = threading.Event()
        raw = self.a59_split_fixture(producer)
        class CancelAfterChunk(io.BytesIO):
            def read1(response, size):
                response.reads += 1
                part = io.BytesIO.read(response, min(7, size))
                cancel.set()
                return part
        response = CancelAfterChunk(raw)
        response.reads = 0
        self.addCleanup(response.close)
        with tp.offline_policy() as capability:
            context = self.context(capability=capability)
            caught = None
            with patch.object(urllib.request.OpenerDirector, 'open', return_value=response) as opener:
                try:
                    splits.guarded_split_fetch(context.worker('a58-body-cancel'), producer,
                        'FTNT' if producer.endswith('issuer') else 'TEST', attempts=3, cancel=cancel,
                        **self.a59_split_options(producer))
                except RequestCancelled as error:
                    caught = error
            self.assertEqual(response.reads, 1, 'A58 no read after in-body cancellation')
            self.assertIsInstance(caught, RequestCancelled)
            self.assertEqual(str(caught), 'split body read cancelled')
            self.assertEqual(opener.call_count, 1)
            self.assertTrue(response.closed)
            context.seal()
        events = inspect_ledger(context.ledger.path)['events']
        self.assertEqual(len(events), 3)
        self.assertEqual(events[1]['payload']['outcome'], 'error')

    def test_a58_split_body_cancel_and_inverse(self):
        inverse = _source_inverse(splits.guarded_split_attempt,
            'if cancel is not None and cancel.is_set():\n'
            '                            raise RequestCancelled("split body read cancelled")', 'if False:\n                            pass')
        for producer, _, _, _ in self._A23B_BINDINGS:
            with self.subTest(producer=producer):
                self.a58_body_cancel_witness(producer)
                with patch.object(splits, 'guarded_split_attempt', inverse):
                    with self.assertRaisesRegex(AssertionError, 'A58 no read after in-body cancellation'):
                        self.a58_body_cancel_witness(producer)
                print('A58_BODY_CANCEL_INVERSE_RED', producer)

    def a58_wire_witness(self, producer, endpoint):
        expected_urls = {
            'sec.tickers': 'https://www.sec.gov/files/company_tickers.json',
            'sec.companyfacts': 'https://data.sec.gov/api/xbrl/companyfacts/CIK0000789019.json',
            'fortinet.history': 'https://www.fortinet.com/corporate/about-us/newsroom/press-releases/2022/fortinet-announces-five-for-one-stock-split',
        }
        expected_caps = {'stockanalysis.history': 8 * 1024 * 1024,
                         'yahoo.splits': 8 * 1024 * 1024, 'sec.tickers': 8 * 1024 * 1024,
                         'sec.companyfacts': 16 * 1024 * 1024, 'fortinet.history': 4 * 1024 * 1024,
                         'sp500.github': 1 * 1024 * 1024, 'sp500.wikipedia': 4 * 1024 * 1024}
        self.assertEqual(dict(ledger_module.HTTP_BODY_CAPS), expected_caps, 'A58 literal production body caps')
        self.assertEqual(dict(splits.HTTP_BODY_CAPS), expected_caps, 'A58 literal bridge body caps')
        with tp.offline_policy() as capability:
            context = self.context(capability=capability)
            options = self.a59_split_options(producer)
            agent = 'Offline fixture fixture@example.invalid' if endpoint.startswith('sec.') else 'EMA-DataBank/1.0'
            expected_headers = {'User-agent': agent, 'Accept-encoding': 'identity',
                                'Accept': 'text/html' if endpoint == 'fortinet.history' else 'application/json'}
            seen = []
            def wire_open(_opener, wire, *, timeout):
                seen.append(wire)
                self.assertEqual(wire.get_method(), 'GET', 'A58 fixed actual wire method')
                self.assertEqual(dict(wire.header_items()), expected_headers, 'A58 fixed actual wire headers')
                parsed = urlsplit(wire.full_url)
                if endpoint == 'yahoo.splits':
                    self.assertEqual((parsed.scheme, parsed.netloc, parsed.path),
                        ('https', 'query1.finance.yahoo.com', '/v8/finance/chart/TEST'), 'A58 fixed Yahoo wire target')
                    start_day = max(date(1980, 1, 1), context.authority.first_date)
                    first_open = next(context.authority.window('1d', start_day + timedelta(days=i))[0]
                        for i in range(7) if context.authority.window('1d', start_day + timedelta(days=i)) is not None)
                    self.assertEqual(dict(parse_qsl(parsed.query)), {
                        'period1': str(int(first_open.timestamp())),
                        'period2': str(int(context.horizons['1d'].timestamp())),
                        'interval': '1d', 'events': 'splits', 'includeAdjustedClose': 'true'}, 'A58 fixed actual Yahoo query')
                    self.assertEqual(len(parse_qsl(parsed.query)), 5)
                else:
                    self.assertEqual(wire.full_url, expected_urls[endpoint], 'A58 fixed metadata wire URL')
                return io.BytesIO(self.a59_split_fixture(producer))
            with patch.object(urllib.request.OpenerDirector, 'open', wire_open):
                splits.guarded_split_attempt(context.worker('a58-wire'), producer,
                    'FTNT' if producer.endswith('issuer') else 'TEST', **options)
            self.assertEqual(len(seen), 1)
            context.seal()
        evidence = inspect_ledger(context.ledger.path)['events'][0]['payload']['http_request']
        self.assertEqual(evidence['url'], seen[0].full_url)
        self.assertEqual(evidence['body_cap_bytes'], expected_caps[endpoint])
        self.assertEqual(evidence['header_digests'], {
            'User-Agent': hashlib.sha256(agent.encode('ascii')).hexdigest(),
            'Accept': hashlib.sha256(expected_headers['Accept'].encode('ascii')).hexdigest(),
            'Accept-Encoding': hashlib.sha256(b'identity').hexdigest()})
        if endpoint == 'yahoo.splits':
            self.assertTrue(evidence['period2_exclusive'])

    def test_a58_split_fixed_wire_values_and_inverses(self):
        for producer, endpoint, _, _ in self._A23B_BINDINGS:
            with self.subTest(endpoint=endpoint):
                self.a58_wire_witness(producer, endpoint)
        for old, new in (
            ('("Accept-Encoding", "identity")', '("Accept-Encoding", "gzip")'),
            ("('period1', int(start.timestamp()))", "('period1', int(start.timestamp()) + 86400)"),
            ("('events', 'splits')", "('events', 'dividends')"),
            ("('interval', '1d')", "('interval', '1wk')"),
            ("('includeAdjustedClose', 'true')", "('includeAdjustedClose', 'false')"),
        ):
            inverse = _source_inverse(splits._spec, old, new)
            with self.subTest(wire_inverse=old), patch.object(splits, '_spec', inverse):
                with self.assertRaisesRegex(AssertionError, 'A58 fixed actual'):
                    self.a58_wire_witness('http.split_provider.yahoo', 'yahoo.splits')
            print('A58_FIXED_WIRE_INVERSE_RED', old)
        for producer, endpoint in (('http.split_provider.yahoo', 'yahoo.splits'),
                                   ('http.split_provider.issuer', 'fortinet.history')):
            with patch.object(splits, '_AGENT', 'Mozilla/5.0'):
                with self.assertRaisesRegex(AssertionError, 'A58 fixed actual wire headers'):
                    self.a58_wire_witness(producer, endpoint)
            print('A58_FIXED_AGENT_INVERSE_RED', endpoint)
        for endpoint in ('sec.tickers', 'fortinet.history'):
            changed = dict(ledger_module.HTTP_BODY_CAPS, **{endpoint: ledger_module.HTTP_BODY_CAPS[endpoint] + 1})
            with patch.object(ledger_module, 'HTTP_BODY_CAPS', changed):
                with self.assertRaisesRegex(AssertionError, 'A58 literal production body caps'):
                    self.a58_wire_witness('http.split_provider.yahoo', 'yahoo.splits')
            print('A58_LITERAL_CAP_INVERSE_RED', endpoint)


    def a58_map_accounting_witness(self, row, mapping, reasons, normalized=0):
        accepted, result = self.a58_metadata_exchange('http.split_provider.sec_tickers', {
            '0': {'ticker': 'TEST', 'cik_str': 789019}, '1': row})
        self.assertEqual(accepted, mapping, 'A58 map row policy peers')
        self.a58_row_accounting(result, 2, len(mapping), reasons, normalized)

    def a58_fact_drop_witness(self, row, reason):
        good = {'val': 2, 'end': '2026-08-03', 'accn': 'good'}
        accepted, result = self.a58_metadata_exchange('http.split_provider.sec_facts',
            self.a58_facts_payload([good, row]))
        self.assertEqual(len(accepted['split_facts']), 1, 'A58 one valid fact peer preserved')
        self.assertEqual(len(accepted['records']), 1)
        self.a58_row_accounting(result, 2, 1, {reason: 1}, payload_identity_normalized=False)

    def test_a58_split_metadata_row_policy_reached_inverses(self):
        map_rows = (
            (splits.normalize_split_metadata, "elif type(row.get('ticker')) is not str or not row['ticker'].isascii():",
             "elif type(row.get('ticker')) is not str:",
             {'ticker': 'ß', 'cik_str': 1}, {'TEST': 789019}, {'invalid_ticker': 1}, 0),
            (splits._metadata_cik, 'if type(value) is not int or not 0 < value <= 9999999999:', 'if False:',
             {'ticker': 'OTHER', 'cik_str': True}, {'TEST': 789019}, {'invalid_cik': 1}, 0),
            (splits.normalize_split_metadata, "row['ticker'].strip(' \\t\\r\\n').upper()", "row['ticker']",
             {'ticker': ' brk-b ', 'cik_str': 1067983}, {'TEST': 789019, 'BRK-B': 1067983}, {}, 1),
            (splits.normalize_split_metadata, "reasons['duplicate_identity'] = reasons.get('duplicate_identity', 0) + 1",
             "reasons['wrong_reason'] = reasons.get('wrong_reason', 0) + 1",
             {'ticker': 'test', 'cik_str': 789019}, {'TEST': 789019}, {'duplicate_identity': 1}, 0),
            (splits._metadata_row_evidence, 'dropped_count=observed - accepted', 'dropped_count=observed',
             {'ticker': 'OTHER', 'cik_str': 1}, {'TEST': 789019, 'OTHER': 1}, {}, 0),
        )
        for function, old, new, row, mapping, reasons, normalized in map_rows:
            with self.subTest(map_inverse=old):
                self.a58_map_accounting_witness(row, mapping, reasons, normalized)
                mutant = _source_inverse(function, old, new)
                with patch.object(splits, function.__name__, mutant):
                    with self.assertRaisesRegex(AssertionError, 'A58 map row policy peers|A58 exact metadata row accounting'):
                        self.a58_map_accounting_witness(row, mapping, reasons, normalized)
                print('A58_MAP_ROW_INVERSE_RED', function.__name__, old)
        for old, new, row, reason in (
            ("for key in ('end', 'filed'):", 'for key in ():',
             {'val': 3, 'end': '2026-08-03T12:00:00', 'accn': 'bad'}, 'invalid_end'),
            ("for key in ('end', 'filed'):", 'for key in ():',
             {'val': 3, 'end': '2026-08-03', 'filed': 'not-a-date', 'accn': 'bad'}, 'invalid_filed'),
            ("for key, limit in (('form', 16), ('accn', 64)):", 'for key, limit in ():',
             {'val': 3, 'end': '2026-08-03', 'form': 'F' * 17, 'accn': 'bad'}, 'invalid_form'),
            ("for key, limit in (('form', 16), ('accn', 64)):", 'for key, limit in ():',
             {'val': 3, 'end': '2026-08-03', 'accn': 'A' * 65}, 'invalid_accn'),
        ):
            with self.subTest(fact_inverse=reason):
                self.a58_fact_drop_witness(row, reason)
                mutant = _source_inverse(splits._fact_drop_reason, old, new)
                with patch.object(splits, '_fact_drop_reason', mutant):
                    with self.assertRaisesRegex(AssertionError, 'A58 one valid fact peer preserved'):
                        self.a58_fact_drop_witness(row, reason)
                print('A58_FACT_ROW_INVERSE_RED', reason)
        # Removing the ratio drop invokes the legacy parser's own drop and
        # then quarantines the entire payload: that loses its good peer.
        bad_ratio = {'val': 0, 'end': '2026-08-03', 'accn': 'bad'}
        self.a58_fact_drop_witness(bad_ratio, 'invalid_ratio')
        mutant = _source_inverse(splits._fact_drop_reason, "return 'invalid_ratio'", 'return None')
        with patch.object(splits, '_fact_drop_reason', mutant):
            with self.assertRaisesRegex(ResponseQuarantined, 'identity-unsafe'):
                self.a58_fact_drop_witness(bad_ratio, 'invalid_ratio')
        print('A58_FACT_RATIO_DROP_INVERSE_RED good peer would be quarantined')


    def a59_collision_witness(self, rows, expected, reasons, *, collision_groups=1):
        accepted, result = self.a58_metadata_exchange('http.split_provider.sec_facts',
                                                     self.a58_facts_payload(rows))
        facts, records = accepted['split_facts'], accepted['records']
        actual = sorted((fact['ratio'], fact['fact_date'], fact['accession']) for fact in facts)
        self.assertEqual(actual, sorted(expected), 'A59 clean filing facts preserved')
        self.assertEqual(set(records), {fact['source_id'] for fact in facts}, 'A59 exact source record set')
        self.assertEqual(len(records), len(facts), 'A59 one record per accepted fact')
        for fact in facts:
            record = records[fact['source_id']]
            self.assertEqual({key: record[key] for key in fact}, fact)
            self.assertEqual(record['payload_source_id'], result['raw_response_digest'])
        self.a58_row_accounting(result, len(rows), len(expected), reasons,
                                payload_identity_normalized=False,
                                colliding_identity_count=collision_groups if
                                reasons.get('source_identity_collision') else 0)
        return accepted, result

    def test_a59_split_filing_local_collision_policy_and_inverses(self):
        clean = {'val': 5, 'end': '2020-08-31', 'accn': 'clean-a'}
        peer = {'val': 3, 'end': '2022-08-31', 'accn': 'clean-b'}
        first = {**clean, 'accn': 'mixed'}
        second = {**peer, 'accn': 'mixed'}
        expected = [(5.0, clean['end'], 'clean-a'), (3.0, peer['end'], 'clean-b')]
        cases = (
            ('two-splits-one-filing', [first, second], [], {'source_identity_collision': 2}),
            ('three-filings-one-colliding', [clean, peer, first, second], expected,
             {'source_identity_collision': 2}),
            ('one-ratio-two-ends', [clean, first, {**first, 'end': '2020-09-30'}], expected[:1],
             {'source_identity_collision': 2}),
            ('true-ratio-conflict', [clean, first, {**first, 'val': 3}], expected[:1],
             {'source_identity_collision': 2}),
            ('reversed-order', [second, first, peer, clean], expected, {'source_identity_collision': 2}),
            ('trimmed-accession', [clean, first, {**second, 'accn': ' mixed '}], expected[:1],
             {'source_identity_collision': 2}),
            ('duplicate-precedence', [clean, first, first, second], expected[:1],
             {'duplicate_fact': 1, 'source_identity_collision': 2}),
            ('invalid-peer-precedence', [clean, first, second, {'val': 0}], expected[:1],
             {'invalid_ratio': 1, 'source_identity_collision': 2}),
            ('two-collision-groups', [clean, first, second,
                                     {**first, 'accn': 'other'}, {**second, 'accn': 'other'}],
             expected[:1], {'source_identity_collision': 4}),
            ('no-accession-distinct', [{**clean, 'accn': None}, {**peer, 'accn': None}],
             [(5.0, clean['end'], None), (3.0, peer['end'], None)], {}),
            ('fallback-format-collision', [clean, {'val': 1.0000001, 'end': '2022-08-31'},
                                           {'val': 1.0000002, 'end': '2022-08-31'}],
             expected[:1], {'source_identity_collision': 2}),
            ('explicit-fallback-collision', [clean, {'val': 2, 'end': '2022-08-31'},
                                            {'val': 3, 'end': '2021-08-31', 'accn': '2022-08-31:2'}],
             expected[:1], {'source_identity_collision': 2}),
        )
        for name, rows, values, reasons in cases:
            with self.subTest(collision=name):
                self.a59_collision_witness(rows, values, reasons,
                    collision_groups=2 if name == 'two-collision-groups' else 1)
        rows = [clean, peer, first, second]
        for old, new, outcome in (
            ('if count > 1}', 'if count > 2}', 'quarantine'),
            ("if source_id not in colliding}", 'if True}', 'quarantine'),
            ("reasons['source_identity_collision'] = len(facts) - len(kept)",
             "reasons['wrong_reason'] = len(facts) - len(kept)", 'accounting'),
        ):
            with self.subTest(collision_inverse=old):
                mutant = _source_inverse(splits.normalize_split_metadata, old, new)
                with patch.object(splits, 'normalize_split_metadata', mutant):
                    if outcome == 'quarantine':
                        with self.assertRaisesRegex(ResponseQuarantined, 'identity-unsafe'):
                            self.a59_collision_witness(rows, expected, {'source_identity_collision': 2})
                    else:
                        with self.assertRaisesRegex(AssertionError, 'A58 exact metadata row accounting'):
                            self.a59_collision_witness(rows, expected, {'source_identity_collision': 2})
                print('A59_COLLISION_INVERSE_RED', old, outcome)
        import split_provider as legacy
        original = legacy.parse_sec_split_facts
        for fault in ('fact', 'record'):
            def faulty(*args, selected=fault):
                facts, records = original(*args)
                if selected == 'fact':
                    kept = facts[:-1]
                    return kept, {fact['source_id']: records[fact['source_id']] for fact in kept}
                return facts, {}
            def loss_witness():
                with self.assertRaisesRegex(ResponseQuarantined, 'identity-unsafe'):
                    self.a58_metadata_exchange('http.split_provider.sec_facts', self.a58_facts_payload([clean]))
            with self.subTest(parser_loss=fault), patch.object(legacy, 'parse_sec_split_facts', faulty):
                loss_witness()
                old = ('if len(facts) != len(sanitized):' if fault == 'fact' else
                       "if len(records) != len(facts) or set(records) != {fact['source_id'] for fact in facts}:")
                mutant = _source_inverse(splits.normalize_split_metadata, old, 'if False:')
                with patch.object(splits, 'normalize_split_metadata', mutant):
                    with self.assertRaisesRegex(AssertionError, 'ResponseQuarantined not raised'):
                        loss_witness()
                print('A59_PARSER_LOSS_INVERSE_RED', fault)

    def test_a59_split_metadata_capture_type_and_inverse(self):
        with tp.offline_policy() as capability:
            context = self.context(capability=capability)
            request = splits.split_request(context.worker('a59-capture'),
                'http.split_provider.sec_tickers', 'TEST',
                **self.a59_split_options('http.split_provider.sec_tickers'))
            raw = json.dumps({'0': {'ticker': 'TEST', 'cik_str': 789019}}).encode()
            class NotExactDict(dict):
                pass
            def witness(capture):
                caught = None
                try:
                    splits.normalize_split_metadata(raw, request.request.envelope, _capture=capture)
                except Exception as error:
                    caught = error
                self.assertIs(type(caught), RequestRefused, 'A59 capture type refused before parsing')
                self.assertEqual(str(caught), 'metadata capture requires an owned dictionary')
            for capture in ([], 'text', 3, False, NotExactDict()):
                with self.subTest(capture_type=type(capture).__name__):
                    witness(capture)
            for capture in ({}, None):
                with self.subTest(valid_capture=type(capture).__name__):
                    self.assertEqual(splits.normalize_split_metadata(raw, request.request.envelope,
                        _capture=capture), {'TEST': 789019})
            mutant = _source_inverse(splits.normalize_split_metadata,
                'if _capture is not None and type(_capture) is not dict:', 'if False:')
            with patch.object(splits, 'normalize_split_metadata', mutant):
                for capture in ([], 'text', 3, False, NotExactDict()):
                    with self.subTest(capture_inverse=type(capture).__name__), self.assertRaisesRegex(
                            AssertionError, 'A59 capture type refused before parsing'):
                        witness(capture)
                    print('A59_CAPTURE_TYPE_INVERSE_RED', type(capture).__name__)
            context.seal()
            self.assertEqual([event['event'] for event in inspect_ledger(context.ledger.path)['events']], ['seal'])

    def test_a59_split_map_vertical_and_formfeed_whitespace_inverses(self):
        for whitespace in ('\v', '\f'):
            row = {'ticker': whitespace + 'OTHER' + whitespace, 'cik_str': 1}
            with self.subTest(whitespace=repr(whitespace)):
                self.a58_map_accounting_witness(row, {'TEST': 789019}, {'invalid_ticker': 1}, 0)
                mutant = _source_inverse(splits.normalize_split_metadata,
                    "row['ticker'].strip(' \\t\\r\\n').upper()", "row['ticker'].strip().upper()")
                with patch.object(splits, 'normalize_split_metadata', mutant), self.assertRaisesRegex(
                        AssertionError, 'A58 map row policy peers'):
                    self.a58_map_accounting_witness(row, {'TEST': 789019}, {'invalid_ticker': 1}, 0)
                print('A59_WHITESPACE_INVERSE_RED', repr(whitespace))


    def a60_provider_exchange(self, ticker, events, facts=(), *, cik=789019,
                              payload_cik=None, issuer=None, provider=None,
                              publish_map=False, now="2026-08-04T17:00:00"):
        """Composition through real guarded wire/executor; no opaque provider patch."""
        import split_provider
        yahoo = {"chart": {"result": [{"events": {"splits": {
            str(index): {"date": int(datetime.fromisoformat(day).replace(
                tzinfo=timezone.utc).timestamp()), "numerator": numerator,
                "denominator": denominator}
            for index, (day, numerator, denominator) in enumerate(events)}}}]}}
        fact_rows = [{"val": ratio, "end": day, "accn": "fixture-" + str(index)}
                     for index, (day, ratio) in enumerate(facts)]
        company = self.a58_facts_payload(fact_rows, cik if payload_cik is None else payload_cik)
        bodies = {
            "query1.finance.yahoo.com": json.dumps(yahoo).encode(),
            "www.sec.gov": json.dumps({"0": {"ticker": ticker, "cik_str": cik}}).encode(),
            "data.sec.gov": json.dumps(company).encode(),
            "www.fortinet.com": issuer if issuer is not None else
                b"<html>This is the second time. The 2011 two-for-one stock split.</html>",
        }
        calls = []
        def wire_open(_opener, wire, *, timeout):
            parsed = urlsplit(wire.full_url)
            calls.append((parsed.netloc, parsed.path))
            return io.BytesIO(bodies[parsed.netloc])
        if provider is None:
            provider = split_provider.YahooSecProvider(attempts=1,
                sec_user_agent="Offline fixture fixture@example.invalid",
                clock=lambda: self.fail("A60 provider must not sample a second clock"),
                sleep_fn=lambda *args: self.fail("A60 no local provider sleep"))
        error = None
        result = None
        with tp.offline_policy() as capability:
            context = self.context(now, capability=capability)
            with patch.object(urllib.request.OpenerDirector, 'open', wire_open):
                try:
                    result = provider.fetch_history(ticker, _fetch_worker=context.worker('a60-provider'))
                except (split_provider.ProviderError, ResponseQuarantined) as caught:
                    error = caught
            self.assertFalse(context.ledger.verified, 'A60 no provider-owned seal')
            if provider._pending_map is not None and error is None:
                before = deepcopy(provider._sealed_map)
                with self.assertRaisesRegex(LedgerError, 'UNVERIFIED'):
                    provider._publish_ticker_map(context)
                self.assertEqual(provider._sealed_map, before, 'A60 pending map cannot publish before seal')
            context.seal()
            if publish_map and error is None:
                provider._publish_ticker_map(context)
        audit = inspect_ledger(context.ledger.path)
        decisions = [event for event in audit['events'] if event['event'] == 'decision']
        results = [event for event in audit['events'] if event['event'] == 'result']
        self.assertEqual(len(calls), len(decisions), 'A60 one physical pair per actual guarded open')
        self.assertEqual(len(results), len(decisions))
        self.assertEqual({event['attempt_id'] for event in decisions},
                         {event['attempt_id'] for event in results})
        self.assertNotIn(b'fixture@example.invalid', context.ledger.path.read_bytes())
        return dict(result=result, error=error, audit=audit, context=context,
                    calls=calls, provider=provider)

    def a60_composition_witness(self):
        ftnt = self.a60_provider_exchange('FTNT',
            [('2011-06-02', 2, 1), ('2022-06-23', 5, 1)], [('2022-06-23', 5)])
        self.assertIsNone(ftnt['error'], 'A60 FTNT history composition succeeded')
        result = ftnt['result']
        self.assertEqual([(event['ex_date'], event['confidence']) for event in result['events']],
            [('2011-06-02', 'confirmed'), ('2022-06-23', 'confirmed')], 'A60 both FTNT confirmations')
        self.assertTrue(result['coverage']['complete'], 'A60 actual issuer completeness')
        self.assertEqual(result['coverage']['evidence_level'], 'issuer_explicit_history')
        self.assertRegex(result['coverage']['complete_basis']['payload_sha256'], r'^[0-9a-f]{64}$')
        self.assertEqual(result['coverage']['through'],
                         ftnt['context'].horizons['1d'].date().isoformat(), 'A60 settled coverage end')
        aapl = self.a60_provider_exchange('AAPL', [('2005-02-28', 2, 1), ('2014-06-09', 7, 1)],
                                           [('2014-06-09', 7)])['result']
        self.assertEqual({result['events'][0]['ex_date'], *[event['ex_date'] for event in aapl['events']]},
                         {'2011-06-02', '2005-02-28', '2014-06-09'}, 'A60 three retained historical events')
        self.assertEqual([event['confidence'] for event in aapl['events']],
                         ['provisional', 'confirmed'], 'A60 uncorroborated event stays provisional')
        self.assertFalse(aapl['coverage']['complete'])
        ge = self.a60_provider_exchange('GE', [('2021-08-02', 1, 8)], [('2021-08-02', .125)])['result']
        self.assertEqual((ge['events'][0]['ratio'], ge['events'][0]['confidence'],
                          ge['events'][0]['ratio_convention']),
                         (.125, 'confirmed', 'new_shares_per_old_share'), 'A60 confirmed new/old reverse ratio')
        for payload in (result, aapl, ge):
            for event in payload['events']:
                for source_id in event['source_ids']:
                    source = payload['sources'][source_id]
                    if 'payload_source_id' in source:
                        self.assertIn(source['payload_source_id'], payload['sources'], 'A60 record links its guarded payload')
        missing = self.a60_provider_exchange('FTNT', [('2022-06-23', 5, 1)], [('2022-06-23', 5)])
        import split_provider
        self.assertIsInstance(missing['error'], split_provider.ProviderError, 'A60 missing issuer event refused')
        self.assertIsNone(missing['result'])
        mapped = self.a60_provider_exchange('MSFT', [])
        self.assertEqual(mapped['result']['cik'], 789019)
        self.assertIn(('www.sec.gov', '/files/company_tickers.json'), mapped['calls'], 'A60 cold map guarded request')
        self.assertIn(('data.sec.gov', '/api/xbrl/companyfacts/CIK0000789019.json'), mapped['calls'], 'A60 resolved identity forwarded')
        bad = self.a60_provider_exchange('BAD', [], cik=123, payload_cik=999)
        self.assertIsInstance(bad['error'], ResponseQuarantined, 'A60 payload CIK mismatch quarantined')
        self.assertIsNone(bad['result'])
        self.assertEqual(bad['audit']['events'][-2]['payload']['outcome'], 'error')
        self.assertEqual(len(list(bad['context'].ledger.path.parent.glob(
            bad['context'].ledger.path.stem + '.*.quarantine.json'))), 1)

    def test_a60_guarded_provider_composition_and_reached_inverses(self):
        import split_provider
        self.a60_composition_witness()
        inverses = (
            ('SEC-confirmation', 'support = _find_support(facts, event)', 'support = None',
             'A60 uncorroborated event stays provisional|A60 confirmed new/old reverse ratio'),
            ('issuer-confirmation', 'if issuer_record_id and _ftnt_issuer_matches(event):', 'if False:',
             'A60 both FTNT confirmations'),
            ('coverage-complete', 'complete=bool(issuer_record_id)', 'complete=False', 'A60 actual issuer completeness'),
            ('event-preservation', 'for row in yahoo_rows:', 'for row in yahoo_rows[-1:]:',
             'A60 FTNT history composition succeeded'),
            ('settled-coverage', 'through=end.date().isoformat()', 'through=(end.date() + dt.timedelta(days=1)).isoformat()',
             'A60 settled coverage end'),
        )
        for name, old, new, oracle in inverses:
            with self.subTest(inverse=name):
                inverse = _source_inverse(split_provider.YahooSecProvider.fetch_history, old, new)
                with patch.object(split_provider.YahooSecProvider, 'fetch_history', inverse):
                    with self.assertRaisesRegex(AssertionError, oracle):
                        self.a60_composition_witness()
                print('A60_COMPOSITION_INVERSE_RED', name)

    def test_a60_provider_pending_sealed_map_custody(self):
        first = self.a60_provider_exchange('MSFT', [], publish_map=True)
        provider = first['provider']
        self.assertIsNone(provider._pending_map)
        self.assertEqual(set(provider._sealed_map['sealed_identity']),
                         {'attempt_id', 'result_seq', 'seal_seq', 'seal_digest'})
        second = self.a60_provider_exchange('MSFT', [], provider=provider)
        self.assertIsNone(second['error'])
        self.assertEqual([host for host, path in second['calls']],
                         ['query1.finance.yahoo.com', 'data.sec.gov'], 'A60 sealed map hit creates no physical pair')
        provider._sealed_map['rows']['MSFT'] = 1
        changed = self.a60_provider_exchange('MSFT', [], provider=provider)
        self.assertIn(('www.sec.gov', '/files/company_tickers.json'), changed['calls'], 'A60 changed map takes fresh guard')
        self.assertEqual(changed['result']['cik'], 789019)
        pending = self.a60_provider_exchange('MSFT', [])
        self.assertIsNone(pending['provider']._sealed_map)
        fresh = self.a60_provider_exchange('MSFT', [], provider=pending['provider'])
        self.assertIn(('www.sec.gov', '/files/company_tickers.json'), fresh['calls'], 'A60 unpromoted map cannot cross operation')
        ttl_provider = self.a60_provider_exchange('MSFT', [], publish_map=True)['provider']
        ttl = self.a60_provider_exchange('MSFT', [], provider=ttl_provider, now='2026-08-04T19:00:00')
        self.assertIn(('www.sec.gov', '/files/company_tickers.json'), ttl['calls'], 'A60 expired map takes fresh guard')

    def test_a60_provider_worker_and_configuration_refusal(self):
        import split_provider
        for value in (None, '', 'EMA local-research', 'fixture@example.invalid\n'):
            provider = split_provider.YahooSecProvider(sec_user_agent=value)
            with tp.offline_policy() as capability:
                context = self.context(capability=capability)
                with patch.object(urllib.request.OpenerDirector, 'open') as opener:
                    with self.assertRaisesRegex(RequestRefused, 'SEC_USER_AGENT'):
                        provider.fetch_history('MSFT', _fetch_worker=context.worker('a60-bad-agent'))
                opener.assert_not_called()
                self.assertEqual(inspect_ledger(context.ledger.path, require_seal=False)['events'], [])
        provider = split_provider.YahooSecProvider(sec_user_agent='fixture@example.invalid')
        with patch.object(urllib.request.OpenerDirector, 'open') as opener:
            with self.assertRaisesRegex(RequestRefused, 'explicit operation worker'):
                provider.fetch_history('MSFT')
        opener.assert_not_called()
        with tp.offline_policy() as capability:
            context = self.context(capability=capability)
            worker = context.worker('a60-expired')
        with patch.object(urllib.request.OpenerDirector, 'open') as opener:
            with self.assertRaisesRegex(AuthorityError, 'capability expired'):
                provider.fetch_history('MSFT', _fetch_worker=worker)
        opener.assert_not_called()
        context.ledger.close()
        with patch.object(urllib.request.OpenerDirector, 'open') as opener:
            with self.assertRaisesRegex(LedgerError, 'sealed/closed'):
                provider.fetch_history('MSFT', _fetch_worker=worker)
        opener.assert_not_called()

    def a60_altered_map_witness(self, altered_identity=False):
        first = self.a60_provider_exchange('MSFT', [], publish_map=True)
        provider = first['provider']
        if altered_identity:
            provider._sealed_map['sealed_identity']['attempt_id'] = 'altered'
        else:
            provider._sealed_map['rows']['MSFT'] = 1
        second = self.a60_provider_exchange('MSFT', [], provider=provider)
        self.assertIn(('www.sec.gov', '/files/company_tickers.json'), second['calls'],
                      'A60 altered map or result identity takes fresh guard')
        self.assertIsNone(second['error'])
        self.assertEqual(second['result']['cik'], 789019)

    def test_a60_provider_map_reached_inverses(self):
        import split_provider
        for altered_identity in (False, True):
            self.a60_altered_map_witness(altered_identity)
            if altered_identity:
                function = split_provider.YahooSecProvider._load_ticker_map
                inverse = _source_inverse(function,
                    'if identity == self._sealed_map.get("sealed_identity"):', 'if True:')
                replacement = inverse
            else:
                function = split_provider.YahooSecProvider._map_value_matches
                inverse = _source_inverse(function,
                    'entry.get("accepted_digest") != digest_value(rows)', 'False')
                replacement = staticmethod(inverse)
            with patch.object(split_provider.YahooSecProvider, function.__name__, replacement):
                with self.assertRaisesRegex(AssertionError, 'A60 altered map or result identity takes fresh guard'):
                    self.a60_altered_map_witness(altered_identity)
            print('A60_MAP_CUSTODY_INVERSE_RED', 'result-identity' if altered_identity else 'accepted-values')

    def a60_collision_count_witness(self):
        rows = [
            {'val': 2, 'end': '2020-08-31', 'accn': 'clean'},
            {'val': 3, 'end': '2020-08-31', 'accn': 'A'},
            {'val': 5, 'end': '2022-08-31', 'accn': 'A'},
            {'val': 7, 'end': '2020-08-31', 'accn': 'B'},
            {'val': 11, 'end': '2021-08-31', 'accn': 'B'},
            {'val': 13, 'end': '2022-08-31', 'accn': 'B'},
            {'val': 3, 'end': '2020-08-31', 'accn': 'A'}, {'val': 0}]
        accepted, result = self.a58_metadata_exchange('http.split_provider.sec_facts',
                                                     self.a58_facts_payload(rows))
        self.assertEqual(set(accepted), {'cik', 'records', 'split_facts'})
        self.a58_row_accounting(result, 8, 1,
            {'duplicate_fact': 1, 'invalid_ratio': 1, 'source_identity_collision': 5},
            payload_identity_normalized=False, colliding_identity_count=2)
        self.assertEqual(result['metadata_rows']['colliding_identity_count'], 2, 'A60 distinct colliding identities, not rows')

    def test_a60_split_provenance_result_durability(self):
        class DictionarySubclass(dict):
            pass
        for value in ([], 'text', 1, True, DictionarySubclass()):
            with self.subTest(provenance_type=type(value).__name__), tp.offline_policy() as capability:
                context = self.context(capability=capability)
                sender = Mock(return_value=self.a59_split_fixture('http.split_provider.sec_tickers'))
                with self.assertRaisesRegex(RequestRefused, 'provenance must be an owned dictionary'):
                    splits.guarded_split_attempt(context.worker('a60-provenance'),
                        'http.split_provider.sec_tickers', 'TEST', sender, _provenance=value,
                        **self.a59_split_options('http.split_provider.sec_tickers'))
                sender.assert_not_called()
                self.assertEqual(inspect_ledger(context.ledger.path, require_seal=False)['events'], [])
        with tp.offline_policy() as capability:
            context = self.context(capability=capability)
            provenance = {}
            raw_capture = {}
            raw = self.a59_split_fixture('http.split_provider.sec_tickers')
            accepted = splits.guarded_split_attempt(context.worker('a60-provenance'),
                'http.split_provider.sec_tickers', 'TEST', lambda url: raw,
                _provenance=provenance, _capture=raw_capture,
                **self.a59_split_options('http.split_provider.sec_tickers'))
            self.assertEqual(set(raw_capture), {'raw_digest', 'observed_bytes'})
            context.seal()
        audit = inspect_ledger(context.ledger.path)
        self.assertEqual(provenance['operation_id'], audit['operation_id'])
        self.assertEqual(provenance['logical_id'], audit['events'][1]['logical_id'])
        self.assertEqual(provenance['accepted_digest'], digest_value(accepted))
        self.assertEqual(provenance['request_provenance'], audit['events'][0]['payload']['http_request'])
        with tp.offline_policy() as capability:
            context = self.context(capability=capability)
            provenance = {}
            def fail_result_fsync(event, stage):
                if event == 'result' and stage == 'fsync':
                    raise OSError('A60 owned result fsync failure')
            context.ledger._fault = fail_result_fsync
            with self.assertRaisesRegex(LedgerError, 'result durability failure'):
                splits.guarded_split_attempt(context.worker('a60-provenance-failure'),
                    'http.split_provider.sec_tickers', 'TEST', lambda url: raw,
                    _provenance=provenance, **self.a59_split_options('http.split_provider.sec_tickers'))
            self.assertEqual(provenance, {}, 'A60 no provenance before durable result')
            self.assertFalse(context.ledger.verified)

    def test_a60_durable_collision_identity_count_and_reached_inverses(self):
        self.a60_collision_count_witness()
        for replacement in ('0', "reasons.get('source_identity_collision', 0)"):
            inverse = _source_inverse(splits.normalize_split_metadata,
                'colliding_identity_count=len(colliding)', 'colliding_identity_count=' + replacement)
            with patch.object(splits, 'normalize_split_metadata', inverse):
                with self.assertRaisesRegex(AssertionError, 'A58 exact metadata row accounting'):
                    self.a60_collision_count_witness()
            print('A60_COLLISION_COUNT_INVERSE_RED', replacement)
        rows = [{'val': index + 1, 'end': '2020-08-31', 'accn': 'one-filing'} for index in range(20000)]
        accepted, result = self.a58_metadata_exchange('http.split_provider.sec_facts', self.a58_facts_payload(rows))
        self.assertEqual(accepted['split_facts'], [])
        self.a58_row_accounting(result, 20000, 0, {'source_identity_collision': 20000},
            payload_identity_normalized=False, colliding_identity_count=1)

    def a52_root_exchange(self, root_name, *, names=('AAPL',), now='2026-08-04T17:00:00',
                          agent='Offline fixture fixture@example.invalid', overrides=None,
                          body_for=None, scan=None, progress=None, fault=None, capability=True,
                          writer=None, bank_names=None, artifact=None, prepare=None,
                          old_good=False, old_caches=None, reference_rows=None, default_scan=False,
                          default_wire_for=None, allow_manifest_race=False, policy_hook=None):
        """Real roots/engine provider/owned opens; retained bounded scratch bank only."""
        import split_cache
        import fetch_operations
        import fetch_ibkr_bridge
        scratch = Path(tempfile.mkdtemp(prefix='row79-a52-'))
        if not hasattr(self, 'a52_retained_roots'):
            self.a52_retained_roots = []
        self.a52_retained_roots.append(str(scratch))
        root, ledgers = scratch / 'bank', scratch / 'ledgers'
        old = {}
        manifests = {}
        paths = {}
        for index, ticker in enumerate(bank_names or names):
            folder = root / ticker
            folder.mkdir(parents=True)
            manifest = dict(folder=ticker, symbol=ticker, conid=100 + index,
                            intervals={'1d': {'months': {}}})
            if prepare is not None:
                prepare(root, ticker, manifest)
            raw = json.dumps(manifest).encode()
            (folder / 'manifest.json').write_bytes(raw)
            identity = split_cache.manifest_identity(root, ticker)
            path = split_cache.cache_path(root, identity)
            path.parent.mkdir(exist_ok=True)
            if old_caches is not None and ticker in old_caches:
                old[ticker] = old_caches[ticker]
            elif old_good:
                history = dict(ticker=ticker, provider_symbol=ticker, cik=123 + index,
                    fetched_at='2026-08-03T21:00:00+00:00', provider='legacy-offline-fixture',
                    provider_status='ok', events=[], sources={'legacy': {'kind': 'fixture',
                    'payload_sha256': 'f' * 64}}, coverage={'from': '1980-01-01',
                    'through': '2026-08-03', 'evidence_level': 'provisional',
                    'complete': False, 'complete_basis': None, 'source_ids': ['legacy']})
                old[ticker] = json.dumps(split_cache.build_cache_payload(identity, history)).encode()
            else:
                old[ticker] = b'old-unverified-fixture-' + ticker.encode()
            path.write_bytes(old[ticker])
            manifests[ticker] = raw
            paths[ticker] = path
        calls, scans, stages, clocks, call_times = [], [], [], [], []
        mapping = json.dumps({str(index): dict(ticker=ticker, cik_str=123 + index)
                              for index, ticker in enumerate(bank_names or names)}).encode()
        reference_rows = reference_rows if reference_rows is not None else [self.row('2026-08-03')]
        expected_reference = {row['t']: [float(row[key]) for key in ('o', 'h', 'l', 'c')] +
                              [int(row['v'])] for row in reference_rows}
        original_scanner = split_cache.seam_scan.scan_ticker_steps
        original_wire_open = urllib.request.OpenerDirector.open
        def wire_open(_opener, wire, *, timeout):
            parsed = urlsplit(wire.full_url)
            calls.append((parsed.netloc, parsed.path))
            call_times.append((parsed.netloc, self.clock.now()))
            if default_wire_for is not None and default_wire_for(parsed):
                return original_wire_open(_opener, wire, timeout=timeout)
            if body_for is not None:
                replacement = body_for(parsed, len(calls))
                if replacement is not None:
                    return replacement if isinstance(replacement, io.BytesIO) else io.BytesIO(replacement)
            if parsed.netloc == 'query1.finance.yahoo.com':
                raw = json.dumps({'chart': {'result': [{'events': {'splits': {
                    'legacy': {'date': int(datetime(2014, 6, 9, tzinfo=timezone.utc).timestamp()),
                               'numerator': 7, 'denominator': 1}}}}]}}).encode()
            elif parsed.netloc == 'www.sec.gov':
                raw = mapping
            elif parsed.netloc == 'data.sec.gov':
                cik = int(parsed.path.split('CIK')[1].split('.')[0])
                raw = json.dumps(self.a58_facts_payload(
                    [{'val': 7, 'end': '2014-06-09', 'accn': 'fixture-one'}], cik)).encode()
            elif parsed.netloc == 'stockanalysis.com':
                raw = json.dumps({'data': reference_rows}).encode()
            elif parsed.netloc == 'www.fortinet.com':
                raw = b'<html>This is the second time. The 2011 two-for-one stock split.</html>'
            else:
                self.fail('A52 unexpected actual host ' + parsed.netloc)
            return io.BytesIO(raw)
        def scanner(scan_root, ticker, *, ref):
            self.assertIsNone(fetch_ibkr_bridge.current_worker(), 'A52 scan bridge authority cleared')
            with self.assertRaisesRegex(AuthorityError, 'observer'):
                fetch_operations.require_root_admission(object())
            self.assertEqual(Path(scan_root), root, 'A52 scan bank identity')
            self.assertEqual(ref, expected_reference,
                             'A52 scanner consumes detached guarded reference')
            scans.append(ticker)
            if default_scan:
                return original_scanner(scan_root, ticker, ref=ref)
            if scan is not None:
                return scan(scan_root, ticker, ref=ref)
            return dict(ticker=ticker, verdict='CLEAN', candidates=[],
                        series_fingerprint=hashlib.sha256(b'').hexdigest())
        def on_progress(stage, ticker):
            self.assertIsNone(fetch_ibkr_bridge.current_worker(), 'A52 progress authority cleared')
            stages.append((stage, ticker))
            if progress is not None:
                progress(stage, ticker, root)
        def clock():
            clocks.append(1)
            return datetime.fromisoformat(now).replace(tzinfo=NY)
        result = error = None
        options = dict(_evidence_dir=ledgers, _authority=self.authority, _clock=clock,
            _governors=self.governors, _fault=fault, _sec_user_agent=agent,
            scan_fn=None if default_scan else scanner, progress=on_progress)
        options.update(overrides or {})
        if artifact is not None:
            options['artifact'] = (root / 'AAPL' / 'manifest.json' if artifact == 'bank'
                                   else scratch / 'refresh.json')
        def invoke():
            if root_name == 'single':
                return split_cache.refresh_ticker(root, names[0], **options)
            if root_name == 'batch':
                return split_cache.refresh_many(root, list(names), **options)
            if root_name == 'cli':
                return split_cache.run_refresh_cli(root, list(names), **options)
            if root_name == 'main':
                cli_options = dict(options)
                cli_options.pop('progress')
                destination = cli_options.pop('artifact', scratch / 'refresh.json')
                rendered = io.StringIO()
                with redirect_stdout(rendered):
                    exit_code = split_cache.main(['--root', str(root), '--refresh', *names,
                        '--artifact', str(destination)], **cli_options)
                value = json.loads(rendered.getvalue())
                value['cli_exit'] = exit_code
                return value
            self.fail('A52 unknown root')
        with patch.object(urllib.request.OpenerDirector, 'open', wire_open), \
                patch.object(split_cache.seam_scan, 'scan_ticker_steps', scanner if default_scan else original_scanner):
            with patch.object(split_cache.storage, '_atomic_write_bytes',
                              wraps=split_cache.storage._atomic_write_bytes) as atomic:
                if writer is not None:
                    atomic.side_effect = lambda path, raw: writer(path, raw, atomic._mock_wraps)
                try:
                    if capability:
                        with tp.offline_policy() as admitted:
                            options['_test_capability'] = admitted
                            if policy_hook is not None:
                                policy_hook(admitted)
                            result = invoke()
                    else:
                        result = invoke()
                except BaseException as caught:
                    error = caught
        audits = [inspect_ledger(path, require_seal=False) for path in ledgers.glob('*.jsonl')]
        # CAS fixtures may deliberately change a manifest's identity externally.
        # Inspect the ORIGINAL target, not a new path derived from that race.
        current = {ticker: path.read_bytes() for ticker, path in paths.items()}
        current_manifests = {ticker: (root / ticker / 'manifest.json').read_bytes() for ticker in old}
        if not allow_manifest_race:
            self.assertEqual(current_manifests, manifests, 'A52 root never changes manifests')
        for path in ledgers.glob('*.jsonl'):
            self.assertNotIn(b'fixture@example.invalid', path.read_bytes())
        return dict(result=result, error=error, calls=calls, scans=scans, stages=stages,
                    clocks=clocks, audits=audits, old=old, current=current,
                    scratch=scratch, root=root, ledgers=ledgers, call_times=call_times,
                    paths=paths, manifests=manifests, current_manifests=current_manifests)

    def a52_root_dns_cross_host_witness(self, *, seed_root='single', inverse_slot=False):
        """Real guarded roots/default open; only finite DNS/socket fixture effects."""
        # Mutants deliberately create extra resolver releases; an unbounded
        # owned fixture avoids unrelated background overflow errors as credit.
        slot = threading.Semaphore(1) if inverse_slot else threading.BoundedSemaphore(1)
        entered, release = threading.Event(), threading.Event()
        lookups, connections, sockets, owned_threads = [], [], [], []
        original_remaining = http_transport.Deadline.remaining
        original_thread = threading.Thread

        def thread(*args, **kwargs):
            worker = original_thread(*args, **kwargs)
            owned_threads.append(worker)
            return worker

        def remaining(deadline):
            # Isolate resolver ownership from scheduler speed. Existing tests
            # still exercise the actual total-attempt deadline and timer.
            original_remaining(deadline)
            if deadline.target.endpoint_id == 'yahoo.splits' and not release.is_set():
                return .02
            return original_remaining(deadline)

        def lookup(target):
            lookups.append((target.endpoint_id, target.host, 443))
            if inverse_slot and len(lookups) > 1:
                raise OSError('A52 inverse reached an unauthorized second resolver')
            if target.endpoint_id == 'yahoo.splits':
                entered.set()
                if not release.wait(30):
                    raise AssertionError('A52 owned Yahoo resolver not released')
            return [(2, 1, 6, '', ('127.0.0.1', 443))]

        class Socket:
            def settimeout(_self, seconds):
                self.assertGreater(seconds, 0)
            def connect(_self, address):
                connections.append(address)
            def close(_self):
                pass

        def make_socket(*_args):
            sockets.append(1)
            return Socket()

        def connection_probe(connection):
            # Retain the actual catalog-address admission and DNS/connection
            # path. Stop the owned fresh-socket fixture before TLS/HTTP send.
            sock = http_transport.connect_resolved(connection._deadline,
                (connection.host, connection.port), connection.timeout)
            sock.close()
            raise OSError('A52 fresh resolved fixture probe')

        def stopped(run, root, expected_endpoint):
            self.assertIsInstance(run['error'], AuthorityError,
                                  'A52 busy DNS is root STOP, never ticker ROW')
            self.assertIn('still occupied', str(run['error']))
            self.assertEqual(run['old'], run['current'],
                             'A52 busy DNS never publishes earlier staging')
            self.assertEqual(len(run['audits']), 1)
            events = run['audits'][0]['events']
            decisions = [e for e in events if e['event'] == 'decision']
            results = [e for e in events if e['event'] == 'result']
            self.assertTrue(results and len(results) == len(decisions),
                            'A52 busy DNS has durable attempt/result pairs')
            self.assertEqual(decisions[-1]['payload']['requested']['endpoint'], expected_endpoint)
            self.assertEqual(results[-1]['payload']['outcome'], 'error')
            self.assertFalse(any(e['event'] == 'seal' for e in events),
                             'A52 STOP cannot masquerade as clean root seal')
            stops = list(run['ledgers'].glob('split-*/root-stop.json'))
            self.assertEqual(len(stops), 1, 'A52 STOP retains one durable root companion')
            stop = json.loads(stops[0].read_bytes())
            self.assertEqual(stop['operation_id'], run['audits'][0]['operation_id'])
            self.assertEqual(stop['fault_type'], 'AuthorityError')
            self.assertFalse(stop['sealed'])
            self.assertTrue(stop['no_clean_success'])
            self.assertFalse(run['audits'][0]['verified'])
            if root != 'single' and expected_endpoint != 'sec.tickers':
                self.assertIn(('candidate_scan', 'AAPL'), run['stages'],
                              'A52 terminal later ticker reached earlier clean staging')

        with (patch.object(http_transport, '_DNS_SLOT', slot),
              patch.object(http_transport, 'threading', SimpleNamespace(Thread=thread,
                  Event=threading.Event, Lock=threading.Lock, Timer=threading.Timer)),
              patch.object(http_transport, '_lookup_addresses', lookup),
              patch.object(http_transport, '_make_socket', make_socket),
              patch.object(http_transport.Deadline, 'remaining', remaining),
              patch.object(http_transport.DeadlineHTTPSConnection, 'connect', connection_probe)):
            try:
                initial_names = ('AAPL',) if seed_root == 'single' else ('AAPL', 'MSFT')
                initial = self.a52_root_exchange(seed_root, names=initial_names, old_good=True,
                    default_wire_for=lambda parsed: parsed.netloc == 'query1.finance.yahoo.com'
                    and (seed_root == 'single' or 'MSFT' in parsed.path.upper()))
                stopped(initial, seed_root, 'yahoo.splits')
                self.assertTrue(entered.is_set())
                retry_ids = {e['attempt_id'] for e in initial['audits'][0]['events']
                    if e['event'] == 'decision'
                    and e['payload']['requested']['endpoint'] == 'yahoo.splits'
                    and e['payload']['requested']['subject'] == initial_names[-1]}
                self.assertEqual(len([e for e in initial['audits'][0]['events']
                                      if e['event'] == 'result' and e['attempt_id'] in retry_ids]), 2,
                                 'A52 timeout then governed busy retry, no third attempt')
                expected_lookup = [('yahoo.splits', 'query1.finance.yahoo.com', 443)]
                self.assertEqual(lookups, expected_lookup)
                for root in ('single', 'batch', 'cli'):
                    for endpoint, host in _A54_TARGETS[:4]:
                        with self.subTest(root_dns=root, endpoint=endpoint):
                            names = ('AAPL',) if root == 'single' else ('AAPL', 'MSFT')
                            def selected(parsed):
                                if parsed.netloc != host:
                                    return False
                                return (root == 'single' or endpoint == 'sec.tickers'
                                    or 'MSFT' in parsed.path.upper() or 'CIK0000000124' in parsed.path.upper())
                            run = self.a52_root_exchange(root, names=names, old_good=True,
                                                        default_wire_for=selected)
                            stopped(run, root, endpoint)
                            self.assertEqual(lookups, expected_lookup,
                                             'A52 busy cross-host roots launch no extra DNS')
                            self.assertEqual((sockets, connections), ([], []),
                                             'A52 busy and late Yahoo never connect')
                            acquired = slot.acquire(blocking=False)
                            if acquired:
                                slot.release()
                            self.assertFalse(acquired, 'A52 only original resolver may free slot')
            finally:
                release.set()
                for worker in owned_threads:
                    worker.join(5)
                    self.assertFalse(worker.is_alive(), 'A52 owned DNS thread actually joined')
                self.assertTrue(slot.acquire(timeout=5), 'A52 original resolver must actually finish')
                slot.release()
            fresh = self.a52_root_exchange('single', old_good=True,
                default_wire_for=lambda parsed: parsed.netloc == 'www.sec.gov')
            self.assertIsNone(fresh['error'], 'A52 fresh resolved fixture failure remains ROW')
            self.assertEqual(fresh['old'], fresh['current'])
            self.assertTrue(fresh['audits'][0]['verified'])
            self.assertEqual(lookups[1:], [('sec.tickers', 'www.sec.gov', 443)] * 2,
                             'A52 fresh SEC root reaches its own catalog host/443')
            self.assertEqual(connections, [('127.0.0.1', 443)] * 2)
            self.assertEqual(len(sockets), 2)
        print('A52_ROOT_DNS_REACHED 12 cross-host/root STOPs; Yahoo timeout/busy retry; fresh SEC own host')

    def a52_root_late_dns_witness(self, root):
        deadlines, lookups, sockets, waits = [], [], [], {}
        original_remaining = http_transport.Deadline.remaining
        def remaining(deadline):
            value = original_remaining(deadline)
            state = waits.get(id(deadline))
            if state is not None:
                state[0] += 1
                if state[0] == 2:
                    # Resolve's wait budget is captured before the fixture
                    # expires the attempt; otherwise a scheduling race tests
                    # the earlier remaining check, not the intended late one.
                    state[1].set()
            return value
        def lookup(target):
            lookups.append((target.host, 443))
            self.assertTrue(waits[id(deadlines[-1])][1].wait(5),
                            'A52 late fixture reached the captured resolver wait budget')
            deadlines[-1].end = http_transport.time.monotonic() - 1
            return [(2, 1, 6, '', ('127.0.0.1', 443))]
        def socket_probe(*_args):
            sockets.append(1)
            raise OSError('A52 late result attempted socket construction')
        def connect(connection):
            deadlines.append(connection._deadline)
            waits[id(connection._deadline)] = [0, threading.Event()]
            return http_transport.connect_resolved(connection._deadline,
                (connection.host, connection.port), connection.timeout)
        with (patch.object(http_transport, '_DNS_SLOT', threading.BoundedSemaphore(1)),
              patch.object(http_transport.Deadline, 'remaining', remaining),
              patch.object(http_transport, '_lookup_addresses', lookup),
              patch.object(http_transport, '_make_socket', socket_probe),
              patch.object(http_transport.DeadlineHTTPSConnection, 'connect', connect)):
            run = self.a52_root_exchange(root, old_good=True,
                default_wire_for=lambda parsed: parsed.netloc == 'query1.finance.yahoo.com')
        self.assertIsNone(run['error'], 'A52 expired attempt remains classified transient ROW')
        self.assertEqual(run['old'], run['current'])
        self.assertEqual(lookups, [('query1.finance.yahoo.com', 443)] * 2)
        self.assertEqual(sockets, [], 'A52 late DNS never begins socket construction')
        self.assertTrue(run['audits'][0]['verified'])
        self.assertEqual(self.a52_root_rows(run, root)[0]['status'], 'error')
        results = [e for e in run['audits'][0]['events'] if e['event'] == 'result']
        self.assertEqual(len(results), 2)
        self.assertTrue(all(e['payload']['outcome'] == 'timeout'
                            and e['payload']['error_type'] == 'TimeoutError' for e in results))

    def a52_root_catalog_dns_witness(self, root):
        lookups = []
        original_lookup = http_transport._lookup_addresses
        def getaddrinfo(host, port, **_kwargs):
            lookups.append((host, port))
            return [(2, 1, 6, '', ('127.0.0.1', 443))]
        def lookup(target):
            # Mint/request guards remain intact. The finite resolver fixture is
            # scoped only inside this admitted lookup and restored before its
            # worker returns; no parallel resolver can hold the process slot.
            with patch.object(http_transport.socket, 'getaddrinfo', getaddrinfo):
                return original_lookup(target)
        def connect(connection):
            http_transport.resolve(connection._deadline)
            raise OSError('A52 owned catalog probe before socket/TLS')
        with (patch.object(http_transport, '_DNS_SLOT', threading.BoundedSemaphore(1)),
              patch.object(http_transport, '_lookup_addresses', lookup),
              patch.object(http_transport, '_make_socket') as socket_probe,
              patch.object(http_transport.DeadlineHTTPSConnection, 'connect', connect)):
            run = self.a52_root_exchange(root, old_good=True,
                default_wire_for=lambda parsed: parsed.netloc == 'www.sec.gov')
            socket_probe.assert_not_called()
        self.assertIsNone(run['error'])
        self.assertEqual(run['old'], run['current'])
        self.assertTrue(run['audits'][0]['verified'])
        self.assertEqual(lookups, [('www.sec.gov', 443)] * 2,
                         'A52 root SEC resolver uses catalog host, not URL text')

    @staticmethod
    def a52_root_rows(run, root):
        return [run['result']] if root == 'single' else run['result']['rows']

    def a52_good_row_witness(self, root, *, expected_opens=None, expected_scans=None, **options):
        import split_cache
        run = self.a52_root_exchange(root, old_good=True, **options)
        self.assertIsNone(run['error'], 'A52 ticker data failure is ROW, not whole-root STOP')
        row = self.a52_root_rows(run, root)[0]
        self.assertEqual(row['status'], 'error', 'A52 rejected ticker is never published')
        self.assertTrue(row['preserved_last_good'], 'A52 usable prior cache is reported preserved')
        self.assertEqual(run['old'], run['current'], 'A52 last-good remains byte-exact')
        self.assertTrue(split_cache.load_cache(run['root'],
            split_cache.manifest_identity(run['root'], row['ticker']),
            now=datetime.fromisoformat('2026-08-04T21:00:00+00:00'))['usable'])
        self.assertTrue(run['audits'][0]['verified'], 'A52 ROW joins and seals owning root')
        self.assertEqual(sum(event['event'] == 'seal' for event in run['audits'][0]['events']), 1)
        if expected_opens is not None:
            self.assertEqual(len(run['calls']), expected_opens, 'A52 exact reached guarded opens')
        if expected_scans is not None:
            self.assertEqual(len(run['scans']), expected_scans, 'A52 exact scan effect')
        return run

    def a52_row_stop_inverse(self, witness):
        import split_cache
        inverse = _source_inverse(split_cache._row_failure, 'return row', 'raise error')
        with patch.object(split_cache, '_row_failure', inverse):
            with self.assertRaisesRegex(AssertionError, 'A52 ticker data failure is ROW'):
                witness('batch')
        print('A52_ROW_TO_STOP_INVERSE_RED actual failed guarded attempt or scan reached')

    def test_a52_split_missing_conid(self):
        def prepare(_root, _ticker, manifest):
            manifest['conid'] = None
        def witness(root):
            run = self.a52_root_exchange(root, prepare=prepare)
            self.assertIsNone(run['error'])
            self.assertEqual(run['calls'], [], 'A52 missing conId refuses before actual opens')
            self.assertEqual(run['scans'], [])
            self.assertEqual(self.a52_root_rows(run, root)[0]['status'], 'error')
            self.assertEqual(run['old'], run['current'])
            return run
        for root in ('single', 'batch', 'cli'):
            with self.subTest(root=root):
                witness(root)
        import split_cache
        inverse = _source_inverse(split_cache._refresh_ticker_body,
            'if identity["conid"] is None:', 'if False:')
        with patch.object(split_cache, '_refresh_ticker_body', inverse):
            with self.assertRaisesRegex(AssertionError, 'A52 missing conId refuses before actual opens'):
                witness('batch')

    def test_a52_split_successful_identity_cache(self):
        import split_cache
        def witness(root):
            run = self.a52_root_exchange(root, old_good=True)
            self.assertIsNone(run['error'], 'A52 successful root requires sealed publication')
            row = self.a52_root_rows(run, root)[0]
            self.assertEqual(row['status'], 'refreshed')
            self.assertEqual(sum(host == 'query1.finance.yahoo.com' for host, _ in run['calls']), 1)
            self.assertEqual(row['conid'], 100)
            loaded = split_cache.load_cache(run['root'],
                split_cache.manifest_identity(run['root'], 'AAPL'),
                now=datetime.fromisoformat('2026-08-04T21:00:00+00:00'))
            self.assertTrue(loaded['usable'])
            self.assertFalse(loaded['fetch_verified'], 'A52 offline cache is not acquisition proof')
            self.assertEqual(loaded['cache']['sealed_fetch']['seal_seq'],
                             run['audits'][0]['events'][-1]['seq'])
            self.assertEqual(loaded['cache']['sealed_fetch']['operation_id'], run['audits'][0]['operation_id'])
            self.assertNotEqual(run['old'], run['current'])
            return run
        for root in ('single', 'batch', 'cli'):
            with self.subTest(root=root):
                witness(root)
        inverse = _source_inverse(split_cache._run_split, 'operation.seal()', 'pass  # inverse: no seal')
        with patch.object(split_cache, '_run_split', inverse):
            with self.assertRaisesRegex(AssertionError, 'A52 successful root requires sealed publication'):
                witness('batch')

    def test_a52_split_provider_unavailable_preserves_good(self):
        def unavailable(parsed, _count):
            if parsed.netloc == 'query1.finance.yahoo.com':
                raise URLError(TimeoutError('A52 bounded fixture unavailable'))
        def witness(root):
            run = self.a52_good_row_witness(root, body_for=unavailable, expected_opens=2, expected_scans=0)
            results = [event for event in run['audits'][0]['events'] if event['event'] == 'result']
            self.assertEqual(len(results), 2)
            self.assertTrue(all(event['payload']['outcome'] == 'error' for event in results))
            self.assertEqual(len({event['logical_id'] for event in results}), 1)
            return run
        for root in ('single', 'batch', 'cli'):
            with self.subTest(root=root):
                witness(root)
        self.a52_row_stop_inverse(witness)

    def test_a52_split_malformed_history_preserves_good(self):
        def malformed(parsed, _count):
            if parsed.netloc == 'query1.finance.yahoo.com':
                return b'{malformed-history'
        def witness(root):
            run = self.a52_good_row_witness(root, body_for=malformed, expected_opens=1, expected_scans=0)
            result = run['audits'][0]['events'][-2]['payload']
            self.assertEqual(result['error_type'], 'ResponseQuarantined')
            self.assertEqual(result['raw_response_digest'], hashlib.sha256(b'{malformed-history').hexdigest())
            paths = list(run['ledgers'].glob('*.quarantine.json'))
            self.assertEqual(len(paths), 1)
            self.assertEqual(base64.b64decode(json.loads(paths[0].read_bytes())['response']['raw_response_base64']),
                             b'{malformed-history')
            return run
        for root in ('single', 'batch', 'cli'):
            with self.subTest(root=root):
                witness(root)
        self.a52_row_stop_inverse(witness)

    def test_a52_split_reference_unavailable_preserves_good(self):
        def unavailable(parsed, _count):
            if parsed.netloc == 'stockanalysis.com':
                raise URLError(TimeoutError('A52 bounded reference unavailable'))
        def witness(root):
            run = self.a52_good_row_witness(root, body_for=unavailable, expected_scans=0)
            results = [event for event in run['audits'][0]['events'] if event['event'] == 'result'
                       and event['producer_id'] == 'http.stockanalysis.validation']
            self.assertTrue(results, 'A52 classified reference error has actual durable results')
            self.assertTrue(all(event['payload']['outcome'] == 'error' for event in results))
            self.assertLessEqual(len(results), 3, 'A52 bounded canonical reference retries')
            return run
        for root in ('single', 'batch', 'cli'):
            with self.subTest(root=root):
                witness(root)
        self.a52_row_stop_inverse(witness)

    def test_a52_split_malformed_reference_before_scan(self):
        def malformed(parsed, _count):
            if parsed.netloc == 'stockanalysis.com':
                return b'{malformed-reference'
        def witness(root):
            run = self.a52_good_row_witness(root, body_for=malformed, expected_opens=4, expected_scans=0)
            result = run['audits'][0]['events'][-2]
            self.assertEqual(result['producer_id'], 'http.stockanalysis.validation')
            self.assertEqual(result['payload']['error_type'], 'ResponseQuarantined')
            self.assertEqual(len(list(run['ledgers'].glob('*.quarantine.json'))), 1)
            return run
        for root in ('single', 'batch', 'cli'):
            with self.subTest(root=root):
                witness(root)
        self.a52_row_stop_inverse(witness)

    def a52_scan_row_witness(self, root, *, verdict='CLEAN', fingerprint=None, candidates=None):
        def scan(_root, ticker, *, ref):
            return dict(ticker=ticker, verdict=verdict, why='A52 retained scan failure',
                series_fingerprint=hashlib.sha256(b'').hexdigest() if fingerprint is None else fingerprint,
                candidates=[] if candidates is None else candidates)
        return self.a52_good_row_witness(root, scan=scan, expected_opens=4, expected_scans=1)

    def a52_scan_verdict_checks(self, verdict):
        import split_cache
        for root in ('single', 'batch', 'cli'):
            with self.subTest(root=root):
                self.a52_scan_row_witness(root, verdict=verdict)
        inverse = _source_inverse(split_cache._detection_from_scan,
            'if verdict not in {"CLEAN", "SEAM"}:', 'if False:')
        with patch.object(split_cache, '_detection_from_scan', inverse):
            with self.assertRaisesRegex(AssertionError, 'A52 rejected ticker is never published'):
                self.a52_scan_row_witness('batch', verdict=verdict)

    def test_a52_split_unverifiable_scan_preserves_good(self):
        self.a52_scan_verdict_checks('UNVERIFIABLE')

    def test_a52_split_short_scan_preserves_good(self):
        self.a52_scan_verdict_checks('SHORT')

    def test_a52_split_unsafe_candidate_identity(self):
        import split_cache
        candidate = dict(ticker='OTHER', date='2014-01-10', boundary='2014-01-10',
            factor=4.0, deep_cv=0.001, current_anchor_sound=True)
        def witness(root):
            return self.a52_scan_row_witness(root, verdict='SEAM', candidates=[candidate])
        for root in ('single', 'batch', 'cli'):
            with self.subTest(root=root):
                witness(root)
        inverse = _source_inverse(split_cache._refresh_ticker_body,
            'payload["detection"] = _detection_from_scan(identity, scan, payload["fetched_at"])',
            'payload["detection"] = _detection_from_scan(identity, dict(scan, candidates=[], verdict="CLEAN"), payload["fetched_at"])')
        with patch.object(split_cache, '_refresh_ticker_body', inverse):
            with self.assertRaisesRegex(AssertionError, 'A52 rejected ticker is never published'):
                witness('batch')

    def test_a52_split_invalid_candidate_fingerprint(self):
        import split_cache
        for fingerprint in ('not-a-digest', 'd' * 64):
            for root in ('single', 'batch', 'cli'):
                with self.subTest(root=root, fingerprint=fingerprint):
                    self.a52_scan_row_witness(root, fingerprint=fingerprint)
        inverse = _source_inverse(split_cache._refresh_ticker_body,
            'payload["detection"] = _detection_from_scan(identity, scan, payload["fetched_at"])',
            'payload["detection"] = _detection_from_scan(identity, dict(scan, series_fingerprint=hashlib.sha256(b" ".strip()).hexdigest()), payload["fetched_at"])')
        with patch.object(split_cache, '_refresh_ticker_body', inverse):
            with self.assertRaisesRegex(AssertionError, 'A52 rejected ticker is never published'):
                self.a52_scan_row_witness('batch', fingerprint='not-a-digest')
        inverse = _source_inverse(split_cache._refresh_ticker_body,
            'if payload["detection"]["series_fingerprint"] != seam_scan.manifest_fingerprint(',
            'if False and payload["detection"]["series_fingerprint"] != seam_scan.manifest_fingerprint(')
        with patch.object(split_cache, '_refresh_ticker_body', inverse):
            with self.assertRaisesRegex(AssertionError, 'A52 rejected ticker is never published'):
                self.a52_scan_row_witness('batch', fingerprint='d' * 64)

    def a52_real_series_witness(self, root):
        import split_cache
        import platform
        rows, grouped = [], {}
        day = date(2010, 1, 1)
        while len(rows) < 240:
            if self.authority.row(day).is_open:
                close = 25.0 if len(rows) < 120 else 100.0
                grouped.setdefault(day.isoformat()[:7], []).append((
                    datetime.combine(day, time()), close, close, close, close, 1000))
                rows.append(dict(t=day.isoformat(), o=100, h=100, l=100, c=100, v=1000))
            day += timedelta(days=1)
        def prepare(bank, ticker, manifest):
            months = manifest['intervals']['1d']['months']
            for month, bars in grouped.items():
                year, number = map(int, month.split('-'))
                path = split_cache.storage.month_file_path(bank, ticker, year, number, '1d')
                months[month] = split_cache.storage.write_month_file(path, bars)
        # Pandas' first import asks platform.machine(); keep the socket audit
        # tripwire active and supply finite fixture OS facts instead of a hostname lookup.
        fixture_uname = platform.uname_result('Windows', 'offline-fixture', '10', 'fixture', 'AMD64')
        with patch.object(platform, '_uname_cache', fixture_uname):
            run = self.a52_root_exchange(root, prepare=prepare, reference_rows=rows,
                                         default_scan=True, old_good=True)
        self.assertIsNone(run['error'], 'A52 real series and default scanner complete')
        row = self.a52_root_rows(run, root)[0]
        self.assertEqual(row['status'], 'refreshed', 'A52 real local series actually publishes')
        self.assertEqual(row['candidate_count'], 1, 'A52 real scanner retains original seam candidate')
        payload = json.loads(run['current']['AAPL'])
        candidate = payload['detection']['candidates'][0]
        self.assertEqual(candidate['factor'], 4.0)
        self.assertTrue(candidate['current_anchor_sound'])
        self.assertEqual(run['scans'], ['AAPL'])
        self.assertEqual(sum(host == 'stockanalysis.com' for host, _ in run['calls']), 1,
                         'A52 default scanner must not fetch the supplied reference again')
        return run

    def test_a52_split_real_temporary_series_scanner(self):
        import split_cache
        for root in ('single', 'batch', 'cli'):
            with self.subTest(root=root):
                self.a52_real_series_witness(root)
        inverse = _source_inverse(split_cache.seam_scan.detect_steps,
            'candidates.sort(key=lambda row: (row["date"], row["factor"]))',
            'candidates.clear()')
        with patch.object(split_cache.seam_scan, 'detect_steps', inverse):
            with self.assertRaisesRegex(AssertionError, 'A52 real local series actually publishes|A52 real scanner retains original seam candidate'):
                self.a52_real_series_witness('batch')

    def a52_ftnt_witness(self, root, *, issuer=None, old_caches=None):
        def body(parsed, _count):
            if parsed.netloc == 'query1.finance.yahoo.com':
                return json.dumps({'chart': {'result': [{'events': {'splits': {
                    str(index): dict(date=int(datetime.fromisoformat(day).replace(
                        tzinfo=timezone.utc).timestamp()), numerator=numerator, denominator=1)
                    for index, (day, numerator) in enumerate((('2011-06-02', 2), ('2022-06-23', 5)))}}}]}}).encode()
            if parsed.netloc == 'data.sec.gov':
                return json.dumps(self.a58_facts_payload([dict(val=5, end='2022-06-23', accn='fixture-2022')], 123)).encode()
            if parsed.netloc == 'www.fortinet.com' and issuer is not None:
                return issuer
        return self.a52_root_exchange(root, names=('FTNT',), body_for=body,
                                     old_good=True, old_caches=old_caches)

    def a52_ftnt_complete_witness(self, root):
        run = self.a52_ftnt_witness(root)
        self.assertIsNone(run['error'])
        row = self.a52_root_rows(run, root)[0]
        self.assertEqual(row['status'], 'refreshed', 'A52 guarded FTNT history publishes')
        self.assertTrue(row['coverage_complete'], 'A52 FTNT actual completeness survives composition')
        cache = json.loads(run['current']['FTNT'])
        self.assertEqual([(event['ex_date'], event['confidence']) for event in cache['events']],
            [('2011-06-02', 'confirmed'), ('2022-06-23', 'confirmed')], 'A52 FTNT both historical confirmations')
        self.assertEqual(cache['coverage']['evidence_level'], 'issuer_explicit_history')
        self.assertRegex(cache['coverage']['complete_basis']['payload_sha256'], r'^[0-9a-f]{64}$')
        self.assertEqual({host for host, _ in run['calls']},
            {'query1.finance.yahoo.com', 'www.sec.gov', 'data.sec.gov', 'www.fortinet.com', 'stockanalysis.com'})
        self.assertEqual(len(cache['sealed_fetch']['sources']), 4)
        return run

    def test_a52_split_ftnt_complete_guarded_cache(self):
        import split_provider
        for root in ('single', 'batch', 'cli'):
            with self.subTest(root=root):
                self.a52_ftnt_complete_witness(root)
        for label, old, new, oracle in (
                ('issuer-confidence', 'if issuer_record_id and _ftnt_issuer_matches(event):', 'if False:',
                 'A52 FTNT both historical confirmations'),
                ('completeness-basis', 'if issuer_record_id:\n        coverage["complete_basis"]',
                 'if False:\n        coverage["complete_basis"]', 'A52 guarded FTNT history publishes')):
            with self.subTest(inverse=label):
                inverse = _source_inverse(split_provider.YahooSecProvider.fetch_history, old, new)
                with patch.object(split_provider.YahooSecProvider, 'fetch_history', inverse):
                    with self.assertRaisesRegex(AssertionError, oracle):
                        self.a52_ftnt_complete_witness('batch')

    def test_a52_split_changed_issuer_preserves_good(self):
        for root in ('single', 'batch', 'cli'):
            with self.subTest(root=root):
                prior = self.a52_ftnt_complete_witness(root)
                run = self.a52_ftnt_witness(root, issuer=b'<html>issuer page changed</html>',
                                           old_caches=prior['current'])
                self.assertIsNone(run['error'])
                row = self.a52_root_rows(run, root)[0]
                self.assertEqual(row['status'], 'error')
                self.assertTrue(row['preserved_last_good'])
                self.assertEqual(run['current'], prior['current'], 'A52 old complete issuer evidence preserved byte-exact')
                self.assertEqual(run['scans'], [])
                self.assertEqual(run['audits'][0]['events'][-2]['producer_id'], 'http.split_provider.issuer')
                self.assertEqual(run['audits'][0]['events'][-2]['payload']['error_type'], 'ResponseQuarantined')
                self.assertEqual(len(list(run['ledgers'].glob('*.quarantine.json'))), 1)

    def a52_three_historical_events_witness(self):
        # Original split-suite composition assertion (47.2), not a parser relabel.
        ftnt = self.a60_provider_exchange('FTNT',
            [('2011-06-02', 2, 1), ('2022-06-23', 5, 1)], [('2022-06-23', 5)])
        aapl = self.a60_provider_exchange('AAPL',
            [('2005-02-28', 2, 1), ('2014-06-09', 7, 1)], [('2014-06-09', 7)])
        retained = {event['ex_date'] for run in (ftnt, aapl)
                    for event in (run['result'] or {}).get('events', [])
                    if event['ex_date'] < '2015-01-01'}
        self.assertEqual(retained, {'2011-06-02', '2005-02-28', '2014-06-09'},
                         'A52 original three pre-2015 events retained')
        self.assertIsNone(ftnt['error'])
        self.assertIsNone(aapl['error'])

    def a52_metadata_composition_witness(self, case):
        if case == 'uncorroborated-AAPL':
            run = self.a60_provider_exchange('AAPL',
                [('2005-02-28', 2, 1), ('2014-06-09', 7, 1)], [('2014-06-09', 7)])
            self.assertIsNone(run['error'], 'A52 AAPL composition succeeds')
            events = run['result']['events']
            self.assertEqual([(event['ex_date'], event['confidence']) for event in events],
                [('2005-02-28', 'provisional'), ('2014-06-09', 'confirmed')],
                'A52 original uncorroborated event is provisional')
            self.assertFalse(run['result']['coverage']['complete'])
        elif case == 'confirmed-GE-reverse':
            run = self.a60_provider_exchange('GE', [('2021-08-02', 1, 8)], [('2021-08-02', .125)])
            self.assertIsNone(run['error'], 'A52 GE composition succeeds')
            event = run['result']['events'][0]
            self.assertEqual((event['ratio'], event['confidence'], event['ratio_convention']),
                (.125, 'confirmed', 'new_shares_per_old_share'), 'A52 original confirmed reverse new/old ratio')
        elif case == 'cold-MSFT-map':
            run = self.a60_provider_exchange('MSFT', [])
            self.assertIsNone(run['error'], 'A52 actual resolved map identity reaches CompanyFacts')
            self.assertIn(('www.sec.gov', '/files/company_tickers.json'), run['calls'],
                          'A52 original cold map makes a guarded map request')
            self.assertEqual(run['result']['cik'], 789019)
            self.assertIn(('data.sec.gov', '/api/xbrl/companyfacts/CIK0000789019.json'), run['calls'])
        else:
            self.fail('unknown A52 retained composition case')
        return run

    def test_a52_split_yahoo_metadata_retry_matrix(self):
        import split_provider
        for case in ('uncorroborated-AAPL', 'confirmed-GE-reverse', 'cold-MSFT-map'):
            with self.subTest(original_composition=case):
                self.a52_metadata_composition_witness(case)
        for case, function, old, new, oracle in (
                ('uncorroborated-AAPL', split_provider.YahooSecProvider.fetch_history,
                 'confidence="confirmed" if len(source_ids) > len(event["source_ids"]) else "provisional"',
                 'confidence="confirmed"', 'A52 original uncorroborated event is provisional'),
                ('confirmed-GE-reverse', split_provider.YahooSecProvider.fetch_history,
                 'events.append(dict(ex_date=row["ex_date"], ratio=row["ratio"],',
                 'events.append(dict(ex_date=row["ex_date"], ratio=1.0 / row["ratio"],',
                 'A52 original confirmed reverse new/old ratio'),
                ('cold-MSFT-map', split_provider.YahooSecProvider._resolve_cik,
                 'mapping, source = self._load_ticker_map(worker, symbol, cancel=cancel)',
                 'mapping, source = ({symbol: 789019}, dict(source_id="invented-map", kind="sec_ticker_map"))',
                 'A52 original cold map makes a guarded map request'),
                ('cold-MSFT-map', split_provider.YahooSecProvider._resolve_cik,
                 'cik = mapping.get(symbol)', 'cik = 123',
                 'A52 actual resolved map identity reaches CompanyFacts')):
            with self.subTest(composition_inverse=case, source_anchor=old):
                inverse = _source_inverse(function, old, new)
                with patch.object(split_provider.YahooSecProvider, function.__name__, inverse):
                    with self.assertRaisesRegex(AssertionError, oracle):
                        self.a52_metadata_composition_witness(case)
        for status, host in ((429, 'query1.finance.yahoo.com'), (503, 'query1.finance.yahoo.com'),
                             (403, 'data.sec.gov')):
            for root in ('single', 'batch', 'cli'):
                attempted = []
                def saturated(parsed, _count):
                    if parsed.netloc == host:
                        attempted.append(self.clock.now())
                        raise HTTPError('https://' + parsed.netloc + parsed.path, status,
                            'fixture saturation', {'Retry-After': '2'}, io.BytesIO(b''))
                with self.subTest(root=root, saturation=status, host=host):
                    run = self.a52_good_row_witness(root, body_for=saturated, expected_scans=0)
                    self.assertEqual(len(attempted), 2)
                    self.assertGreaterEqual(attempted[1] - attempted[0], 2.0,
                                            'A52 retries honor shared saturation deadline')
                    results = [event for event in run['audits'][0]['events'] if event['event'] == 'result'
                               and event['payload'].get('http_error', {}).get('status') == status]
                    self.assertEqual(len(results), 2)
                    self.assertEqual(len({event['logical_id'] for event in results}), 1)

    def a52_missing_ftnt_event_witness(self, root):
        def missing(parsed, _count):
            if parsed.netloc == 'query1.finance.yahoo.com':
                return json.dumps({'chart': {'result': [{'events': {'splits': {'one': dict(
                    date=int(datetime(2022, 6, 23, tzinfo=timezone.utc).timestamp()),
                    numerator=5, denominator=1)}}}]}}).encode()
        run = self.a52_root_exchange(root, names=('FTNT',), body_for=missing, old_good=True)
        self.assertIsNone(run['error'], 'A52 missing issuer event is explicit ROW')
        row = self.a52_root_rows(run, root)[0]
        self.assertEqual(row['status'], 'error', 'A52 issuer completeness cannot invent missing FTNT event')
        self.assertTrue(row['preserved_last_good'])
        self.assertEqual(run['old'], run['current'])
        self.assertEqual(run['scans'], [])
        self.assertTrue(run['audits'][0]['verified'])
        self.assertIn(('www.fortinet.com', '/corporate/about-us/newsroom/press-releases/2022/fortinet-announces-five-for-one-stock-split'),
                      run['calls'])
        return run

    def a52_payload_cik_mismatch_witness(self, root):
        def wrong(parsed, _count):
            if parsed.netloc == 'data.sec.gov':
                return json.dumps(self.a58_facts_payload([], 999)).encode()
        binding = {'cik': 123} if root == 'single' else {'cik_bindings': {'AAPL': 123}}
        run = self.a52_good_row_witness(root, overrides=binding, body_for=wrong,
                                       expected_opens=2, expected_scans=0)
        result = run['audits'][0]['events'][-2]
        self.assertEqual(result['producer_id'], 'http.split_provider.sec_facts')
        self.assertEqual(result['payload']['error_type'], 'ResponseQuarantined',
                         'A52 original payload CIK mismatch quarantines actual bytes')
        self.assertEqual(len(list(run['ledgers'].glob('*.quarantine.json'))), 1)
        return run

    def a52_registry_root_witness(self, root):
        names = ('AAPL',) if root == 'single' else ('AAPL', 'MSFT')
        run = self.a52_root_exchange(root, names=names, old_good=True)
        self.assertIsNone(run['error'], 'A52 registry admits the actual registered root with its required rights')
        self.assertEqual([row['status'] for row in self.a52_root_rows(run, root)],
                         ['refreshed'] * len(names))
        self.assertEqual(run['clocks'], [1])
        self.assertEqual(len(run['audits']), 1)
        self.assertTrue(run['audits'][0]['verified'])
        self.assertEqual(sum(event['event'] == 'seal' for event in run['audits'][0]['events']), 1)
        return run

    def a52_reviewed_dialog_source_witness(self, source):
        current = ('                    result = split_cache.refresh_ticker(\n'
                   '                        self._storage_root, ticker,\n')
        previous = ('                    from split_provider import YahooSecProvider\n'
                    '                    provider = YahooSecProvider(timeout=30)\n'
                    '                    result = split_cache.refresh_ticker(\n'
                    '                        self._storage_root, ticker, provider,\n')
        self.assertEqual(source.count(current), 1, 'A52 reviewed exact GUI dispatch shape')
        reconstructed = source.replace(current, previous, 1)
        self.assertEqual(hashlib.sha256(reconstructed.encode()).hexdigest(),
            'ccd92d446981c8d55e7b1eba185034e1a1bbc79f94aaed73ad39c6e4ec006a0f',
            'A52 WHOLE GUI source contains only the reviewed 45.5 delta')

    def a52_extracted_dialog_function(self, source, names, namespace):
        owner = ast.parse(source)
        for name in names:
            found = [node for node in owner.body
                     if isinstance(node, (ast.ClassDef, ast.FunctionDef)) and node.name == name]
            self.assertEqual(len(found), 1, 'A52 exact extracted dialog owner')
            owner = found[0]
        module = ast.Module(body=[deepcopy(owner)], type_ignores=[])
        ast.fix_missing_locations(module)
        local = dict(namespace)
        exec(compile(module, str(Path(__file__).resolve().parent.parent / 'display_data.py'), 'exec'), local)
        return local[names[-1]]

    def a52_dialog_render_witness(self, source, bank, rejected_row):
        import queue
        import split_cache
        messages, finished = [], []
        q = queue.Queue()
        environment = dict(self=SimpleNamespace(_storage_root=bank,
                root=SimpleNamespace(after=Mock(side_effect=AssertionError('A52 no real GUI scheduling')))),
            ticker='AAPL', q=q, split_cache=split_cache, refresh_cancel=threading.Event(),
            export_quality=SimpleNamespace(ensure_current_health=Mock(
                side_effect=AssertionError('A52 failed refresh must not schedule health'))),
            finish=lambda: finished.append(True), say=messages.append,
            health_report=SimpleNamespace(summarize_report=Mock(
                side_effect=AssertionError('A52 failed refresh has no health report'))))
        work_names = ('DataViewerApp', '_storage_verify_dialog', 'run_split_refresh', 'work')
        poll_names = ('DataViewerApp', '_storage_verify_dialog', 'poll')
        # This extracted source executes the shipped default-held root. It does
        # not launch a thread/window or borrow an admitted GUI capability.
        self.a52_extracted_dialog_function(source, work_names, environment)()
        self.a52_extracted_dialog_function(source, poll_names, environment)()
        self.assertEqual(len(messages), 1)
        self.assertTrue(messages[0].startswith('FAILED: RequestRefused('),
                        'A52 actual held root renders FAILED: RequestRefused')
        self.assertIn('not activated for production', messages[0])
        self.assertEqual(finished, [True])
        # Renderer-only success of carrying a REAL guarded rejected-ticker row.
        # No stub fetch is counted as an admitted GUI production positive.
        environment['split_cache'] = SimpleNamespace(refresh_ticker=lambda *_args, **_kwargs: rejected_row)
        messages.clear()
        finished.clear()
        self.a52_extracted_dialog_function(source, work_names, environment)()
        self.a52_extracted_dialog_function(source, poll_names, environment)()
        self.assertEqual(len(messages), 1)
        self.assertIn('Split refresh failed for AAPL:', messages[0])
        self.assertIn('last-good preserved=True', messages[0],
                      'A52 actual rejected row renders retained last-good truth')
        self.assertEqual(finished, [True])

    def test_a52_split_registry_source_and_dialog_pins(self):
        import fetch_operation_inventory as operation_inventory
        import fetch_operations as operations
        import split_provider
        stock = 'engine/stock_ibkr.py'
        prior_roots = {
            (stock, 'nightly_gap_fill'): 'nightly',
            (stock, 'nightly_gap_fill_parallel'): 'nightly',
            (stock, 'nightly_gap_fill_parallel_resilient'): 'nightly',
            (stock, 'nightly_update'): 'nightly',
            (stock, 'add_stocks_gap_fill'): 'add_stocks',
            (stock, 'add_stocks_gap_fill_parallel_resilient'): 'add_stocks',
            (stock, 'find_symbol'): 'identity', (stock, 'company_lookup'): 'identity',
            (stock, 'validate_symbols'): 'identity', (stock, 'doctor'): 'diagnostic',
            (stock, 'estimate_backfill'): 'diagnostic', (stock, 'probe_max_span'): 'diagnostic',
            (stock, 'fill_missing_days'): 'repair',
            ('engine/live_spot_probe.py', 'run_live_probe'): 'diagnostic',
            ('engine/fix_data_pipeline.py', 'run'): 'repair',
            ('engine/live_internal_revalidate.py', 'run_internal_revalidate'): 'diagnostic',
            ('engine/live_pipeline_check.py', 'run_pipeline_check'): 'diagnostic',
            ('engine/live_internal_daily_check.py', 'run_internal_daily_check'): 'diagnostic',
            ('engine/live_feature_check.py', 'run_feature_check'): 'diagnostic',
            ('engine/live_combined_flags.py', 'run_combined_flags'): 'diagnostic',
            ('engine/live_kind_smoke.py', 'run_kind_smoke'): 'diagnostic',
            ('engine/two_account_pacing_probe.py', 'run_pacing_probe'): 'diagnostic',
            ('engine/live_run_all.py', 'run_all'): 'diagnostic',
            ('engine/live_validate_catchup.py', 'run_catchup'): 'diagnostic',
            **{('engine/stock_validate.py', owner): 'validation' for owner in
               ('validate_series', 'cross_validate_ticker', 'revalidate_library',
                'fetch_daily_reference_single_request')},
            ('engine/external_sweep.py', 'sweep_bank'): 'external_sweep',
            ('engine/external_sweep.py', 'sweep_ticker'): 'external_sweep',
        }
        added = {('engine/split_cache.py', name): 'split_refresh' for name in
                 ('refresh_ticker', 'refresh_many', 'run_refresh_cli')}
        membership = {('engine/sp500.py', name): 'membership' for name in
                      ('current_sp500', 'fetch_current_sp500')}
        daily_repair = {('engine/daily_bar_repair.py', 'run_daily_repair'): 'daily_repair'}
        expected_roots = prior_roots | added | membership | daily_repair
        self.assertEqual(len(prior_roots), 30)
        self.assertEqual(dict(operations.PRODUCTION_ROOTS), expected_roots,
                         'Original30 plus three split, two membership and one date-only repair root')
        self.assertEqual(operation_inventory.ROOT_OWNERS, expected_roots)
        rights = {'http.split_provider.yahoo', 'http.split_provider.sec_tickers',
                  'http.split_provider.sec_facts', 'http.split_provider.issuer',
                  'http.stockanalysis.validation'}
        self.assertEqual(set(operations.PURPOSE_PRODUCERS),
            {'nightly', 'add_stocks', 'repair', 'diagnostic', 'identity',
             'validation', 'external_sweep', 'split_refresh', 'membership', 'daily_repair'})
        self.assertEqual(set(operations.PURPOSE_PRODUCERS['daily_repair']),
                         {'ibkr.gap_fill.session_fetch'}, 'Date-only repair has no HTTP rights')
        self.assertEqual(set(operations.PURPOSE_PRODUCERS['split_refresh']), rights,
                         'A52 exactly five split-purpose rights')
        self.assertEqual(set(operations.PURPOSE_PRODUCERS['membership']),
                         {'http.sp500.github', 'http.sp500.wikipedia'}, 'A64 exactly two membership rights')
        for purpose in ('nightly', 'identity', 'add_stocks'):
            self.assertFalse(any(item.startswith('http.') for item in operations.PURPOSE_PRODUCERS[purpose]))
        self.assertFalse(any(item.startswith('http.') for item in operations._SHARED))
        sources = operation_inventory.source_catalog()
        actual = operation_inventory.build_inventory(sources)
        self.assertEqual(len(actual['production_roots']), 36)
        self.assertEqual(len(actual['production_dispatches']), 40)
        self.assertEqual({(row['path'], row['owner']): row['purpose'] for row in
                          actual['production_roots']}, expected_roots)
        expected_dispatches = {
            ('engine/daily_bar_repair.py', 'main', 'run_daily_repair', 'daily_repair', 'root'),
            ('engine/split_cache.py', 'main', 'run_refresh_cli', 'split_refresh', 'root'),
            ('display_data.py', 'DataViewerApp._storage_verify_dialog.run_split_refresh.work',
             'refresh_ticker', 'split_refresh', 'root'),
            ('display_data.py', 'DataViewerApp._current_sp500_async._work',
             'current_sp500', 'membership', 'root'),
        }
        actual_dispatches = {(row['path'], row['owner'], row['api'], row['purpose'], row['mode'])
                             for row in actual['production_dispatches']}
        self.assertTrue(expected_dispatches <= actual_dispatches)
        for root, owner in (('current', 'current_sp500'), ('fetch', 'fetch_current_sp500')):
            def witness():
                run = self.a64_membership_exchange(root)
                self.assertIsNone(run['error'], 'A64 registry admits actual membership root')
                self.assertEqual(len(run['calls']), 1)
                self.assertTrue(run['audits'][0]['verified'])
            witness()
            changed = dict(operations.PRODUCTION_ROOTS)
            changed.pop(('engine/sp500.py', owner))
            with patch.object(operations, 'PRODUCTION_ROOTS', changed):
                with self.assertRaisesRegex(AssertionError, 'A64 registry admits actual membership root'):
                    witness()
            print('A64_REGISTRY_INVERSE_RED', root, 'root-registration')
        for right in ('http.sp500.github', 'http.sp500.wikipedia'):
            changed = dict(operations.PURPOSE_PRODUCERS,
                membership=frozenset({'http.sp500.github', 'http.sp500.wikipedia'} - {right}))
            with patch.object(operations, 'PURPOSE_PRODUCERS', changed):
                with self.assertRaisesRegex(AssertionError, 'A64 registry admits actual membership root'):
                    witness()
            print('A64_REGISTRY_INVERSE_RED membership required-right', right)
        for root in ('single', 'batch', 'cli'):
            self.a52_registry_root_witness(root)
            key = ('engine/split_cache.py', {'single': 'refresh_ticker', 'batch': 'refresh_many',
                                           'cli': 'run_refresh_cli'}[root])
            changed = dict(operations.PRODUCTION_ROOTS)
            changed.pop(key)
            with patch.object(operations, 'PRODUCTION_ROOTS', changed):
                with self.assertRaisesRegex(AssertionError, 'A52 registry admits the actual registered root'):
                    self.a52_registry_root_witness(root)
            print('A52_REGISTRY_INVERSE_RED', root, 'root-registration')
        for right in sorted(rights):
            changed = dict(operations.PURPOSE_PRODUCERS,
                           split_refresh=frozenset(rights - {right}))
            with patch.object(operations, 'PURPOSE_PRODUCERS', changed):
                with self.assertRaisesRegex(AssertionError, 'A52 registry admits the actual registered root'):
                    self.a52_registry_root_witness('batch')
            print('A52_REGISTRY_INVERSE_RED batch required-right', right)
        held = []
        for root in ('single', 'batch', 'cli'):
            run = self.a52_root_exchange(root, capability=False, old_good=True)
            self.assertIsInstance(run['error'], RequestRefused)
            self.assertEqual(run['clocks'], [])
            self.assertEqual(run['audits'], [])
            self.assertEqual(run['calls'], [])
            self.assertEqual(run['old'], run['current'])
            held.append(run)
        changed = sources['engine/split_cache.py'].replace('result = run_refresh_cli(', 'result = refresh_many(', 1)
        self.assertNotEqual(changed, sources['engine/split_cache.py'])
        with self.assertRaisesRegex(ValueError, 'unregistered root dispatch owner'):
            operation_inventory.build_inventory(dict(sources, **{'engine/split_cache.py': changed}))
        print('A52_REGISTRY_INVERSE_RED cli source-dispatch')

        gui_source = sources['display_data.py'].replace('\r\n', '\n')
        self.a52_reviewed_dialog_source_witness(gui_source)
        changed = gui_source.replace('Split refresh failed for {ticker}:', 'Split refresh successful for {ticker}:', 1)
        with self.assertRaisesRegex(AssertionError, 'A52 WHOLE GUI source contains only the reviewed 45.5 delta'):
            self.a52_reviewed_dialog_source_witness(changed)
        print('A52_REGISTRY_INVERSE_RED gui whole-reviewed-delta')
        row_run = self.a52_fault_row_witness('single', 'malformed')
        rejected_row = row_run['result']
        self.a52_dialog_render_witness(gui_source, held[0]['root'], rejected_row)
        for label, old, new, oracle in (
            ('FAILED-text', 'f"FAILED: {payload}"', 'f"OK: {payload}"',
             'A52 actual held root renders FAILED: RequestRefused'),
            ('last-good-truth', "bool(refresh.get('preserved_last_good'))", 'False',
             'A52 actual rejected row renders retained last-good truth')):
            self.assertEqual(gui_source.count(old), 1)
            with self.assertRaisesRegex(AssertionError, oracle):
                self.a52_dialog_render_witness(gui_source.replace(old, new, 1), held[0]['root'], rejected_row)
            print('A52_REGISTRY_INVERSE_RED gui', label)

        raw_source = Path(split_provider.__file__).read_text(encoding='utf-8')
        retired_key = 'engine.split_provider:_default_fetcher:http_urlopen:1'
        self.assertNotIn(retired_key, fetch_send_inventory.SITE_CLASSIFICATIONS)
        self.assertFalse(fetch_send_inventory.scan_source(raw_source, 'engine.split_provider'))
        for value in ('https://query1.finance.yahoo.com/', object()):
            with self.assertRaises(RequestRefused):
                split_provider._default_fetcher(value, headers=object(), timeout=object())
        inverse = _source_inverse(split_provider._default_fetcher, 'refuse_a2()', 'return None')
        with patch.object(split_provider, '_default_fetcher', inverse):
            with self.assertRaises(AssertionError):
                with self.assertRaises(RequestRefused):
                    split_provider._default_fetcher('https://query1.finance.yahoo.com/')
        print('A52_REGISTRY_INVERSE_RED legacy refusal-only-entry')
        restored = raw_source.replace('    refuse_a2()\n',
            '    refuse_a2()\n    import urllib.request\n    return urllib.request.urlopen(url)\n', 1)
        self.assertEqual([row['site_key'] for row in
                          fetch_send_inventory.scan_source(restored, 'engine.split_provider')], [retired_key])
        sites = fetch_send_inventory.build_inventory()
        self.assertEqual((sites['candidate_count'], sites['logical_producer_count']), (71, 60))
        self.assertEqual(sites['primitive_counts']['http_urlopen'], 3,
                         'A64 retired raw membership seam becomes an owned physical open')
        self.assertEqual(sites['primitive_counts']['http_opener_open'], 3)
        self.assertEqual(sites['disposition_counts']['a2_refused'], 32)
        self.assertNotIn(retired_key, {row['site_key'] for row in sites['sites']})
        self.assertEqual(len(actual['test_mint_sites']), 1)
        self.assertEqual((actual['test_mint_sites'][0]['path'], actual['test_mint_sites'][0]['owner']),
                         ('engine/fetch_a2_ibkr_workflows_selftest.py', 'Operations.open'))
        print('A52_A64_REGISTRY_SOURCE_REACHED original30+3+2 roots;39 dispatches;split5/member2 rights;held/raw retirement;extracted dialogs only')

    def a52_fault_row_witness(self, root, case):
        """Bad FIRST ticker/valid NEXT ticker through actual guarded roots."""
        from contextlib import ExitStack
        first = 'FTNT' if case.startswith('issuer-') else 'AAPL'
        names = (first,) if root == 'single' else (first, 'MSFT')
        attempts = []
        raw_for_quarantine = []
        def target(parsed):
            return (parsed.path.endswith('/' + first) or
                    '/' + first.lower() + '/' in parsed.path.lower() or
                    'CIK0000000123.json' in parsed.path)
        def body(parsed, _count):
            selected = target(parsed)
            yahoo = parsed.netloc == 'query1.finance.yahoo.com' and selected
            facts = parsed.netloc == 'data.sec.gov' and selected
            reference = parsed.netloc == 'stockanalysis.com' and selected
            raw = None
            if yahoo and case in {'malformed', 'duplicate', 'nonfinite', 'oversize'}:
                event = dict(date=int(datetime(2014, 6, 9, tzinfo=timezone.utc).timestamp()),
                             numerator=7, denominator=1)
                events = {'a': event, 'b': event} if case == 'duplicate' else {
                    'a': dict(event, numerator=float('inf'))}
                raw = (b'{malformed-matrix' if case == 'malformed' else b'x' * 500 if case == 'oversize'
                       else json.dumps({'chart': {'result': [{'events': {'splits': events}}]}}).encode())
            elif case == 'unmapped' and parsed.netloc == 'www.sec.gov':
                return b'{"0":{"ticker":"MSFT","cik_str":124}}'
            elif yahoo and case in {'404', '410', '429', '503'} or facts and case == '403':
                status = int(case)
                attempts.append((parsed.netloc, self.clock.now()))
                raise HTTPError('https://' + parsed.netloc + parsed.path, status,
                    'A52 matrix HTTP failure', {'Retry-After': '2'}, io.BytesIO(b''))
            elif facts and case == 'payload-cik':
                raw = json.dumps(self.a58_facts_payload([], 999)).encode()
            elif yahoo and case.startswith('issuer-'):
                events = [('2022-06-23', 5)] if case == 'issuer-missing' else [
                    ('2011-06-02', 2), ('2022-06-23', 5)]
                return json.dumps({'chart': {'result': [{'events': {'splits': {
                    str(index): dict(date=int(datetime.fromisoformat(day).replace(tzinfo=timezone.utc).timestamp()),
                                     numerator=ratio, denominator=1)
                    for index, (day, ratio) in enumerate(events)}}}]}}).encode()
            elif facts and case.startswith('issuer-'):
                return json.dumps(self.a58_facts_payload([{'val': 5, 'end': '2022-06-23', 'accn': 'fixture'}])).encode()
            elif parsed.netloc == 'www.fortinet.com' and case == 'issuer-changed':
                raw = b'<html>changed issuer page without captured completeness basis</html>'
            elif yahoo and case == 'transport' or reference and case == 'reference-transport':
                attempts.append((parsed.netloc, self.clock.now()))
                raise URLError(TimeoutError('A52 matrix bounded provider unavailable'))
            elif reference and case == 'malformed-reference':
                raw = b'{malformed-matrix-reference'
            if raw is not None:
                raw_for_quarantine.append(raw[:257] if case == 'oversize' else raw)
            return raw
        def scan(_bank, ticker, *, ref):
            value = dict(ticker=ticker, verdict='CLEAN', candidates=[],
                         series_fingerprint=hashlib.sha256(b'').hexdigest())
            if ticker == first:
                if case in {'SHORT', 'UNVERIFIABLE'}:
                    value['verdict'] = case
                elif case == 'candidate-identity':
                    value.update(verdict='SEAM', candidates=[dict(ticker='OTHER', date='2014-01-10',
                        boundary='2014-01-10', factor=4.0, deep_cv=.001, current_anchor_sound=True)])
                elif case == 'candidate-fingerprint':
                    value['series_fingerprint'] = 'd' * 64
            return value
        with ExitStack() as stack:
            if case == 'oversize':
                caps = dict(ledger_module.HTTP_BODY_CAPS, **{'yahoo.splits': 256})
                stack.enter_context(patch.object(splits, 'HTTP_BODY_CAPS', caps))
                stack.enter_context(patch.object(ledger_module, 'HTTP_BODY_CAPS', caps))
            run = self.a52_root_exchange(root, names=names, body_for=body, scan=scan, old_good=True)
        self.assertIsNone(run['error'], 'A52 matrix ticker data failure is ROW, not STOP')
        if root == 'main':
            self.assertEqual(run['result'].get('kind'), 'split_cache_refresh',
                             'A52 matrix ticker data failure is ROW, not STOP')
        rows = self.a52_root_rows(run, root)
        self.assertEqual([row['ticker'] for row in rows], list(names))
        self.assertEqual([row['status'] for row in rows],
                         ['error'] + ([] if root == 'single' else ['refreshed']),
                         'A52 matrix rejected first ticker, valid NEXT ticker published')
        self.assertTrue(rows[0]['preserved_last_good'])
        self.assertEqual(run['current'][first], run['old'][first])
        if root != 'single':
            self.assertEqual(run['result']['failures'], 1)
            self.assertIn('MSFT', run['scans'], 'A52 matrix NEXT ticker reaches actual scan')
            self.assertNotEqual(run['current']['MSFT'], run['old']['MSFT'])
            payload = json.loads(run['current']['MSFT'])
            self.assertEqual(payload['sealed_fetch']['operation_id'], run['audits'][0]['operation_id'])
            if root == 'main':
                self.assertEqual(run['result']['cli_exit'], 1, 'A52 matrix command ROW exits1')
                self.assertEqual(run['result']['kind'], 'split_cache_refresh')
                self.assertEqual(json.loads((run['scratch'] / 'refresh.json').read_bytes())['failures'], 1)
        self.assertTrue(run['audits'][0]['verified'])
        self.assertEqual(sum(event['event'] == 'seal' for event in run['audits'][0]['events']), 1)
        quarantines = list(run['ledgers'].glob('*.quarantine.json'))
        if raw_for_quarantine:
            self.assertEqual(len(quarantines), 1, 'A52 matrix actual rejected bytes quarantined once')
            raw = base64.b64decode(json.loads(quarantines[0].read_bytes())['response']['raw_response_base64'])
            self.assertEqual(raw, raw_for_quarantine[0], 'A52 matrix retains exact OBSERVED bytes')
            rejected = [event for event in run['audits'][0]['events'] if event['event'] == 'result'
                        and event['payload'].get('error_type') == 'ResponseQuarantined']
            self.assertEqual(len(rejected), 1)
            self.assertEqual(rejected[0]['payload']['raw_response_digest'], hashlib.sha256(raw).hexdigest())
        if case in {'404', '410', 'transport', 'reference-transport', '429', '503', '403'}:
            expected = 1 if case in {'404', '410', 'reference-transport'} else 2
            self.assertEqual(len(attempts), expected, 'A52 matrix exact bounded physical retries')
            if case in {'429', '503', '403'}:
                self.assertGreaterEqual(attempts[1][1] - attempts[0][1], 2.0)
                if root != 'single':
                    next_times = [stamp for (host, path), (_same, stamp) in zip(run['calls'], run['call_times'])
                                  if host == attempts[-1][0] and ('MSFT' in path or 'CIK0000000124' in path)]
                    self.assertTrue(next_times)
                    self.assertGreaterEqual(next_times[0] - attempts[-1][1], 2.0,
                                            'A52 matrix NEXT ticker honors shared saturation')
        if case == 'unmapped':
            self.assertFalse(any('CIK0000000123' in path for _, path in run['calls']))
        print('A52_FAULT_ROW_REACHED', root, case)
        return run

    def a52_fault_stop_witness(self, root, case):
        """Whole-root STOP after a clean staged prefix when the phase permits."""
        from contextlib import ExitStack, contextmanager
        import split_cache
        import fetch_operations
        names = ('AAPL',) if root == 'single' else ('AAPL', 'MSFT')
        target = names[-1]
        second_started, policies, effects = [], [], []
        mono = [0.0]
        primary = (KeyboardInterrupt('A52 matrix non-Exception STOP') if case == 'non-exception'
                   else RequestCancelled('A52 matrix cancellation STOP') if case == 'cancel'
                   else LedgerError('A52 matrix typed scan STOP'))
        def body(parsed, _count):
            selected = (parsed.path.endswith('/' + target) or
                        ('CIK0000000123' if root == 'single' else 'CIK0000000124') in parsed.path)
            if selected and parsed.netloc == 'query1.finance.yahoo.com':
                second_started.append(True)
                if case in {'cancel', 'non-exception'}:
                    raise primary
                if case == 'policy-expired':
                    policies[0]._live = False
                if case in {'quarantine-fsync', 'quarantine-bytes'}:
                    return b'{malformed-STOP-matrix'
                if case == 'response-close':
                    class Response(io.BytesIO):
                        def close(inner):
                            super().close()
                            effects.append('response-close')
                            raise OSError('A52 matrix response close failed')
                    return Response(b'{malformed-but-primary-cleanup-is-STOP')
            if selected and parsed.netloc == 'data.sec.gov' and case == 'root-deadline':
                mono[0] = 1.0
            return None
        def fault(kind, stage):
            expected = {'result-fsync': 'result', 'seal-fsync': 'receipt'}.get(case)
            if expected == kind and stage == 'fsync' and (root == 'single' or
                    case == 'seal-fsync' or second_started):
                effects.append(kind)
                raise OSError('A52 matrix ' + case)
        def scan(_bank, ticker, *, ref):
            if ticker == target and case == 'typed-scan':
                effects.append('typed-scan')
                raise primary
            return dict(ticker=ticker, verdict='CLEAN', candidates=[],
                        series_fingerprint=hashlib.sha256(b'').hexdigest())
        baseline_roots, baseline_children = set(fetch_operations._OPERATIONS), set(fetch_operations._CHILDREN)
        options = dict(names=names, body_for=body, scan=scan, fault=fault, old_good=True,
                       policy_hook=lambda policy: policies.append(policy))
        if case == 'pre-settlement':
            options['now'] = '2026-08-04T15:00:00'
        if case == 'missing-agent':
            options['agent'] = None
        if case == 'invalid-agent':
            options['agent'] = 'no usable contact'
        if case == 'root-deadline':
            options['overrides'] = {'_budget_seconds': .1}
        with ExitStack() as stack:
            if case == 'root-deadline':
                stack.enter_context(patch.object(split_cache.time, 'monotonic', lambda: mono[0]))
            if case == 'quarantine-fsync':
                stack.enter_context(patch.object(FetchLedger, 'quarantine', return_value=None))
            if case == 'quarantine-bytes':
                original_quarantine = FetchLedger.quarantine
                def quarantine(ledger, attempt_id, response):
                    path = original_quarantine(ledger, attempt_id, response)
                    self.assertIsNotNone(path, 'A52 matrix real fixture quarantine persisted first')
                    retained = json.loads(path.read_bytes())
                    retained['response']['raw_response_base64'] = base64.b64encode(
                        b'different-owned-fixture-bytes').decode('ascii')
                    path.write_bytes(json.dumps(retained).encode())
                    effects.append('quarantine-bytes')
                    return path
                stack.enter_context(patch.object(FetchLedger, 'quarantine', quarantine))
            if case == 'busy-dns':
                slot = threading.BoundedSemaphore(1)
                slot.acquire()
                stack.enter_context(patch.object(http_transport, '_DNS_SLOT', slot))
                dns = stack.enter_context(patch.object(http_transport, '_lookup_addresses'))
                socket = stack.enter_context(patch.object(http_transport, '_make_socket'))
                def connect(connection):
                    effects.append('busy-dns-connect')
                    http_transport.resolve(connection._deadline)
                    raise AssertionError('A52 busy resolver fixture unexpectedly passed refusal')
                stack.enter_context(patch.object(http_transport.DeadlineHTTPSConnection, 'connect', connect))
                options['default_wire_for'] = lambda parsed: parsed.netloc == 'query1.finance.yahoo.com'
            if case == 'child-close':
                original_scope = fetch_operations.scoped_worker
                @contextmanager
                def scope(child, *, close=False):
                    with original_scope(child, close=close) as worker:
                        selected = worker.worker_id == 'split-history-' + str(len(names) - 1)
                        yield worker
                    if selected:
                        effects.append('child-close')
                        raise LedgerError('A52 matrix drained child close failed')
                stack.enter_context(patch.object(fetch_operations, 'scoped_worker', scope))
            run = self.a52_root_exchange(root, **options)
            if case == 'busy-dns':
                dns.assert_not_called()
                socket.assert_not_called()
        if root == 'main' and case != 'non-exception':
            self.assertIsNone(run['error'])
            self.assertEqual(run['result']['kind'], 'split_cache_command_error',
                             'A52 matrix command safety fault is STOP, never a ROW')
            self.assertEqual(run['result']['cli_exit'], 1)
            self.assertFalse((run['scratch'] / 'refresh.json').exists())
            detail = run['result']['error']
        else:
            self.assertIsNotNone(run['error'], 'A52 matrix safety fault is terminal STOP')
            self.assertIsNone(run['result'])
            self.assertIsInstance(run['error'], (AuthorityError, LedgerError, RequestCancelled, KeyboardInterrupt))
            if case in {'typed-scan', 'cancel', 'non-exception'}:
                self.assertIs(run['error'], primary)
            detail = type(run['error']).__name__ + ': ' + str(run['error'])
        self.assertEqual(run['old'], run['current'], 'A52 matrix STOP publishes NO staged prefix')
        self.assertEqual(set(fetch_operations._OPERATIONS), baseline_roots)
        self.assertEqual(set(fetch_operations._CHILDREN), baseline_children,
                         'A52 matrix STOP actually drains all owned children')
        self.assertFalse(any(audit['verified'] for audit in run['audits']))
        if case in {'pre-settlement', 'missing-agent', 'invalid-agent'}:
            self.assertEqual(run['calls'], [], 'A52 matrix preflight STOP before every open')
        if case == 'root-deadline':
            self.assertIn('root deadline expired', detail)
        if case == 'policy-expired':
            self.assertIn('capability expired', detail)
        if case == 'busy-dns':
            self.assertIn('DNS resolver is still occupied', detail)
            self.assertEqual(effects, ['busy-dns-connect'], 'A52 busy resolver actual connection boundary reached once')
        if case == 'quarantine-bytes':
            self.assertIn('split quarantine bytes differ from rejected result', detail)
        if case in {'result-fsync', 'seal-fsync', 'response-close', 'child-close', 'typed-scan', 'quarantine-bytes'}:
            self.assertTrue(effects, 'A52 matrix intended failure boundary reached')
        if root != 'single' and case not in {'pre-settlement', 'missing-agent', 'invalid-agent', 'busy-dns'}:
            self.assertIn('AAPL', run['scans'], 'A52 matrix clean prefix staged before STOP')
        print('A52_FAULT_STOP_REACHED', root, case)
        return run

    def a52_quarantine_none_guard_witness(self, root):
        import fetch_run_context as run_context
        reached = []
        def locator(path):
            self.assertIsNone(path, 'A52 missing quarantine fixture sends only None to locator inverse')
            reached.append(path)
            raise LedgerError('HTTP response quarantine failed; owned locator refusal')
        with patch.object(run_context, '_portable_quarantine_locator', locator):
            run = self.a52_fault_stop_witness(root, 'quarantine-fsync')
        self.assertEqual(reached, [], 'A52 missing quarantine refuses BEFORE evidence locator')
        print('A52_FAULT_NONE_GUARD_REACHED', root)
        return run

    def a52_fault_binding_witness(self, root, case):
        import split_cache
        original = splits.split_request
        reached = []
        def request(worker, producer, symbol, **options):
            retained = original(worker, producer, symbol, **options)
            if producer == 'http.split_provider.yahoo':
                reached.append(case)
                if case in {'endpoint', 'subject', 'variant'}:
                    envelope = replace(retained.request.envelope,
                        **{case: {'endpoint': 'stockanalysis.history', 'subject': 'OTHER',
                                 'variant': 'http-metadata'}[case]})
                    retained = replace(retained, request=replace(retained.request, envelope=envelope))
                elif case == 'right':
                    retained = replace(retained, request=replace(retained.request,
                        producer_id='http.stockanalysis.validation'))
                elif case == 'url':
                    retained = replace(retained, evidence=replace(retained.evidence,
                        url='https://adversarial.invalid/'))
                elif case == 'provenance':
                    retained = replace(retained, evidence=replace(retained.evidence, symbol='OTHER'))
                elif case == 'authority':
                    changed = replace(worker.context, operation_id='forged-context-operation')
                    retained = replace(retained, request=replace(retained.request,
                        worker=replace(worker, context=changed)))
            return retained
        with patch.object(splits, 'split_request', request):
            run = self.a52_root_exchange(root, old_good=True)
        self.assertEqual(reached, [case], 'A52 root actual request-binding corruption reached')
        self.assertEqual(run['calls'], [], 'A52 root wrong request binding refuses BEFORE open')
        self.assertEqual(run['scans'], [])
        self.assertEqual(run['old'], run['current'])
        if root == 'main':
            self.assertIsNone(run['error'])
            self.assertEqual(run['result']['kind'], 'split_cache_command_error')
            self.assertEqual(run['result']['cli_exit'], 1)
            self.assertIn('split retry request binding differs', run['result']['error'])
        else:
            self.assertIsInstance(run['error'], AuthorityError)
            self.assertIn('split retry request binding differs', str(run['error']))
        self.assertFalse(run['audits'][0]['verified'])
        print('A52_FAULT_BINDING_REACHED', root, case)
        return run

    def test_a52_split_fault_class_matrix(self):
        # Retain the original controls, then integrate the full fault categories
        # through failed-first/clean-next and clean-prefix/terminal-next roots.
        import split_provider
        import split_cache
        for root in ('single', 'batch', 'cli'):
            def unmapped(parsed, _count):
                if parsed.netloc == 'www.sec.gov':
                    return b'{"0":{"ticker":"OTHER","cik_str":123}}'
            with self.subTest(root=root, map='valid-but-no-requested-CIK'):
                self.a52_good_row_witness(root, body_for=unmapped, expected_opens=2, expected_scans=0)
            for shape in ('duplicate', 'nonfinite'):
                event = dict(date=int(datetime(2014, 6, 9, tzinfo=timezone.utc).timestamp()),
                             numerator=7, denominator=1)
                events = {'a': event, 'b': event} if shape == 'duplicate' else {
                    'a': dict(event, numerator=float('inf'))}
                raw = json.dumps({'chart': {'result': [{'events': {'splits': events}}]}}).encode()
                def malformed_data(parsed, _count):
                    if parsed.netloc == 'query1.finance.yahoo.com':
                        return raw
                with self.subTest(root=root, history_shape=shape):
                    run = self.a52_good_row_witness(root, body_for=malformed_data,
                                                   expected_opens=1, expected_scans=0)
                    self.assertEqual(run['audits'][0]['events'][-2]['payload']['error_type'], 'ResponseQuarantined')
                    self.assertEqual(len(list(run['ledgers'].glob('*.quarantine.json'))), 1)
            for case in ('missing-enumerated-FTNT', 'payload-CIK-mismatch'):
                with self.subTest(root=root, original_composition=case):
                    (self.a52_missing_ftnt_event_witness if case == 'missing-enumerated-FTNT'
                     else self.a52_payload_cik_mismatch_witness)(root)
            for status in (404, 410):
                def gone(parsed, _count):
                    if parsed.netloc == 'query1.finance.yahoo.com':
                        raise HTTPError('https://' + parsed.netloc + parsed.path, status,
                                        'fixture removed', {}, io.BytesIO(b''))
                with self.subTest(root=root, status=status):
                    self.a52_good_row_witness(root, body_for=gone, expected_opens=1, expected_scans=0)
            for event in ('result', 'receipt'):
                second_reached = []
                def identify_second(parsed, _count):
                    if parsed.netloc == 'query1.finance.yahoo.com' and parsed.path.endswith('/MSFT'):
                        second_reached.append(True)
                def fault(kind, stage):
                    if kind == event and stage == 'fsync' and (root == 'single' or
                            event == 'receipt' or second_reached):
                        raise OSError('A52 terminal persistence fixture ' + event)
                with self.subTest(root=root, persistence=event):
                    run = self.a52_root_exchange(root, fault=fault, old_good=True,
                        names=('AAPL',) if root == 'single' else ('AAPL', 'MSFT'),
                        body_for=identify_second)
                    self.assertIsInstance(run['error'], LedgerError)
                    self.assertEqual(run['old'], run['current'])
                    self.assertFalse(run['audits'][0]['verified'])
                    if root != 'single':
                        self.assertIn('AAPL', run['scans'], 'A52 clean sibling staged before terminal persistence')
            def malformed(parsed, _count):
                if parsed.netloc == 'query1.finance.yahoo.com' and (
                        root == 'single' or parsed.path.endswith('/MSFT')):
                    return b'{malformed'
            with self.subTest(root=root, quarantine='write-refused'), \
                    patch.object(FetchLedger, 'quarantine', return_value=None):
                run = self.a52_root_exchange(root, body_for=malformed, old_good=True,
                    names=('AAPL',) if root == 'single' else ('AAPL', 'MSFT'))
                self.assertIsInstance(run['error'], LedgerError)
                self.assertIn('quarantine', str(run['error']))
                self.assertEqual(run['old'], run['current'])
                self.assertFalse(run['audits'][0]['verified'])
                if root != 'single':
                    self.assertIn('AAPL', run['scans'], 'A52 clean sibling staged before quarantine STOP')
            entry = {'single': 'refresh_ticker', 'batch': 'refresh_many', 'cli': 'run_refresh_cli'}[root]
            inverse = _source_inverse(getattr(split_cache, entry),
                'fops.begin_operation("split_refresh",', 'fops.begin_operation("identity",')
            with self.subTest(root=root, authorization='wrong-root-purpose'), \
                    patch.object(split_cache, entry, inverse):
                run = self.a52_root_exchange(root, old_good=True)
                self.assertIsInstance(run['error'], AuthorityError)
                self.assertEqual(run['calls'], [], 'A52 wrong purpose refuses before actual opens')
                self.assertEqual(run['clocks'], [])
                self.assertEqual(run['audits'], [])
                self.assertEqual(run['old'], run['current'])
        inverse = _source_inverse(split_provider.YahooSecProvider.fetch_history,
            '_require_ftnt_complete_events(events)', 'pass  # inverse: missing required issuer events')
        with patch.object(split_provider.YahooSecProvider, 'fetch_history', inverse):
            with self.assertRaisesRegex(AssertionError, 'A52 issuer completeness cannot invent missing FTNT event'):
                self.a52_missing_ftnt_event_witness('batch')
        inverse = _source_inverse(splits.normalize_split_metadata,
            'if payload_cik != expected_cik:', 'if False:')
        with patch.object(splits, 'normalize_split_metadata', inverse):
            with self.assertRaisesRegex(AssertionError, 'A52 rejected ticker is never published'):
                self.a52_payload_cik_mismatch_witness('batch')

        row_cases = ('malformed', 'duplicate', 'nonfinite', 'oversize', 'unmapped', '404', '410',
            'payload-cik', 'issuer-missing', 'issuer-changed', 'SHORT', 'UNVERIFIABLE',
            'candidate-identity', 'candidate-fingerprint', 'transport', 'reference-transport',
            'malformed-reference', '429', '503', '403')
        stop_cases = ('pre-settlement', 'missing-agent', 'invalid-agent', 'typed-scan', 'cancel',
            'non-exception', 'response-close', 'child-close', 'root-deadline', 'policy-expired',
            'quarantine-fsync', 'quarantine-bytes', 'result-fsync', 'seal-fsync', 'busy-dns')
        binding_cases = ('endpoint', 'subject', 'variant', 'right', 'url', 'provenance', 'authority')
        for root in ('single', 'batch', 'cli', 'main'):
            for case in row_cases:
                with self.subTest(matrix_ROW=case, root=root):
                    self.a52_fault_row_witness(root, case)
            for case in stop_cases:
                with self.subTest(matrix_STOP=case, root=root):
                    self.a52_fault_stop_witness(root, case)
            for case in binding_cases:
                with self.subTest(matrix_binding=case, root=root):
                    self.a52_fault_binding_witness(root, case)
            with self.subTest(matrix_postseal_debt=root):
                self.a52_publication_write_witness(root, 'before')
                print('A52_FAULT_PUBLICATION_REACHED', root, 'exact-debt')
            with self.subTest(matrix_quarantine_none_guard=root):
                self.a52_quarantine_none_guard_witness(root)

        inverse = _source_inverse(split_cache._row_failure, 'return row', 'raise error')
        with patch.object(split_cache, '_row_failure', inverse):
            for case in ('malformed', 'unmapped', '404', 'payload-cik', 'issuer-missing', 'SHORT', 'transport', '429'):
                with self.subTest(matrix_ROW_to_STOP_inverse=case):
                    with self.assertRaisesRegex(AssertionError, 'A52 matrix ticker data failure is ROW, not STOP'):
                        self.a52_fault_row_witness('batch', case)
                    print('A52_FAULT_INVERSE_RED batch ROW-to-STOP', case)
        inverse = _source_inverse(split_cache._refresh_ticker_body,
            'except (AuthorityError, LedgerError, RequestCancelled):\n        raise',
            'except (AuthorityError, LedgerError, RequestCancelled) as error:\n'
            '        return _row_failure(ticker, error, path=path, prior_usable=prior_usable)')
        with patch.object(split_cache, '_refresh_ticker_body', inverse):
            for root in ('single', 'batch', 'cli', 'main'):
                with self.subTest(matrix_STOP_to_ROW_inverse=root):
                    with self.assertRaisesRegex(AssertionError,
                            'A52 matrix (safety fault is terminal STOP|command safety fault is STOP)'):
                        self.a52_fault_stop_witness(root, 'typed-scan')
                    print('A52_FAULT_INVERSE_RED', root, 'STOP-to-ROW')
        inverse = _source_inverse(split_cache._require_quarantined_row,
            'audit = inspect_ledger(context.ledger.path, require_seal=False)',
            'return  # inverse: skip actual rejected-byte evidence custody')
        with patch.object(split_cache, '_require_quarantined_row', inverse):
            for root in ('single', 'batch', 'cli', 'main'):
                with self.subTest(matrix_quarantine_custody_NO_ACTION=root):
                    self.a52_fault_stop_witness(root, 'quarantine-fsync')
                    print('A52_FAULT_INVERSE_NO_ACTION', root, 'earlier-executor-quarantine-guard')
                with self.subTest(matrix_quarantine_custody_inverse=root):
                    with self.assertRaisesRegex(AssertionError,
                            'A52 matrix (safety fault is terminal STOP|command safety fault is STOP)'):
                        self.a52_fault_stop_witness(root, 'quarantine-bytes')
                    print('A52_FAULT_INVERSE_RED', root, 'quarantine-custody')
        import fetch_run_context as run_context
        inverse = _source_inverse(run_context.LogicalRequest._execute,
            'if quarantine_path is None:\n'
            '                    raise LedgerError("HTTP response quarantine failed") from error',
            'pass  # inverse: missing-quarantine guard only; retain locator refusal')
        with patch.object(run_context.LogicalRequest, '_execute', inverse):
            for root in ('single', 'batch', 'cli', 'main'):
                with self.subTest(matrix_executor_quarantine_guard_inverse=root):
                    with self.assertRaisesRegex(AssertionError,
                            'A52 missing quarantine refuses BEFORE evidence locator'):
                        self.a52_quarantine_none_guard_witness(root)
                    print('A52_FAULT_INVERSE_RED', root, 'executor-quarantine-guard')
        inverse = _source_inverse(splits.guarded_split_attempt,
            'retained.evidence != evidence', 'False')
        with patch.object(splits, 'guarded_split_attempt', inverse):
            for root in ('single', 'batch', 'cli', 'main'):
                with self.subTest(matrix_retained_binding_inverse=root):
                    with self.assertRaisesRegex(AssertionError,
                            'A52 root wrong request binding refuses BEFORE open'):
                        self.a52_fault_binding_witness(root, 'url')
                    print('A52_FAULT_INVERSE_RED', root, 'retained-binding')
        print('A52_FAULT_MATRIX_REACHED 176 new positive rows;24 intended inverses;4 disclosed NO_ACTION;15 categories x three roots plus actual command')

    def test_a52_split_caller_objects_refused(self):
        class Bomb:
            def __getattribute__(self, name):
                raise AssertionError('A52 opaque descriptor was inspected')
        for root in ('single', 'batch', 'cli'):
            with self.subTest(root=root, state='production-held'):
                run = self.a52_root_exchange(root, capability=False,
                    overrides={'provider': Bomb(), 'reference_fetcher': Bomb()})
                self.assertIsInstance(run['error'], RequestRefused)
                self.assertEqual(run['clocks'], [])
                self.assertEqual(run['audits'], [])
                self.assertEqual(run['calls'], [])
                self.assertEqual(run['old'], run['current'])
            for option in ('provider', 'reference_fetcher'):
                with self.subTest(root=root, option=option):
                    run = self.a52_root_exchange(root, overrides={option: Bomb()})
                    self.assertIsInstance(run['error'], RequestRefused)
                    self.assertEqual(run['calls'], [])
                    self.assertEqual(run['scans'], [])
                    self.assertEqual(run['clocks'], [1])
                    self.assertEqual(run['old'], run['current'])

    def test_a52_split_pre_settlement_all_roots(self):
        calendars = [('midnight', '2026-08-04T00:00:00', False),
            ('pre-open', '2026-08-04T09:00:00', False),
            ('mid-session', '2026-08-04T12:00:00', False),
            ('close-before-margin', '2026-08-04T16:19:59', False),
            ('settled-boundary', '2026-08-04T16:20:00', True),
            ('after-settle', '2026-08-04T17:00:00', True),
            ('weekend', '2026-08-08T12:00:00', True),
            ('early-close', '2026-07-02T13:19:59', False)]
        for root in ('single', 'batch', 'cli'):
            for label, now, allowed in calendars:
                with self.subTest(root=root, calendar=label):
                    run = self.a52_root_exchange(root, now=now)
                    if not allowed:
                        self.assertIsInstance(run['error'], RequestRefused)
                        self.assertIn('settlement', str(run['error']))
                        self.assertEqual(run['calls'], [], 'A52 preflight zero split opens')
                        self.assertEqual(run['scans'], [])
                        self.assertEqual(run['old'], run['current'])
                        self.assertFalse(any(audit['verified'] for audit in run['audits']))
                    else:
                        self.assertIsNone(run['error'])
                        rows = ([run['result']] if root == 'single' else run['result']['rows'])
                        self.assertEqual([row['status'] for row in rows], ['refreshed'])
                        self.assertEqual(len(run['calls']), 4)
                        self.assertEqual(sum(event['event'] == 'seal'
                            for audit in run['audits'] for event in audit['events']), 1)
                    self.assertEqual(run['clocks'], [1], 'A52 one captured civil clock')
            for label, agent in [('absent', None), ('empty', ''),
                    ('legacy', 'EMA-DataBank/1.0 contact:local-user'),
                    ('control', 'x\nfixture@example.invalid'),
                    ('nonascii', '\u2603 fixture@example.invalid'),
                    ('oversize', 'x' * 240 + ' fixture@example.invalid'),
                    ('no-contact', 'local agent'), ('valid', 'Fixture fixture@example.invalid')]:
                with self.subTest(root=root, sec_config=label):
                    run = self.a52_root_exchange(root, agent=agent)
                    if label != 'valid':
                        self.assertIsInstance(run['error'], RequestRefused)
                        self.assertIn('SEC_USER_AGENT', str(run['error']))
                        self.assertEqual(run['calls'], [])
                        self.assertEqual(run['scans'], [])
                        self.assertEqual(run['old'], run['current'])
                    else:
                        self.assertIsNone(run['error'])
                        decisions = [event for audit in run['audits'] for event in audit['events']
                                     if event['event'] == 'decision' and event['producer_id'].startswith('http.split_provider.sec_')]
                        self.assertTrue(decisions)
                        for event in decisions:
                            self.assertEqual(event['payload']['http_request']['header_digests']['User-Agent'],
                                             hashlib.sha256(agent.encode()).hexdigest())
        import split_cache
        def zero_open_witness(root):
            run = self.a52_root_exchange(root, now='2026-08-04T12:00:00')
            self.assertIsInstance(run['error'], RequestRefused)
            self.assertIn('settlement', str(run['error']))
            self.assertEqual(run['calls'], [], 'A52 root preflight precedes every split open')
            self.assertEqual(run['old'], run['current'])
        for root in ('single', 'batch', 'cli'):
            with self.subTest(preflight_positive=root):
                zero_open_witness(root)
        inverse = _source_inverse(split_cache._run_split, '_reference_preflight(context)',
                                  'pass  # inverse: no required-reference preflight')
        with patch.object(split_cache, '_run_split', inverse):
            for root in ('single', 'batch', 'cli'):
                with self.subTest(preflight_inverse=root):
                    with self.assertRaisesRegex(AssertionError, 'A52 root preflight precedes every split open'):
                        zero_open_witness(root)
        print('A52_PREFLIGHT_INVERSE_RED three roots reach split opens before nested reference refusal')

    def test_a52_split_selected_progress_order(self):
        for root in ('single', 'batch', 'cli'):
            with self.subTest(root=root):
                run = self.a52_root_exchange(root)
                self.assertIsNone(run['error'])
                self.assertEqual(run['stages'], [(stage, 'AAPL') for stage in
                    ('identity', 'provider_history', 'daily_reference', 'candidate_scan', 'commit', 'done')])
                cache = json.loads(run['current']['AAPL'])
                self.assertEqual(cache['sealed_fetch']['operation_id'], run['audits'][0]['operation_id'])
                self.assertEqual(cache['sealed_fetch']['seal_seq'], run['audits'][0]['events'][-1]['seq'])
                self.assertEqual(cache['events'][0]['confidence'], 'confirmed')

    def a52_publication_stop(self, run, *, sealed, staged=None):
        self.assertIsNotNone(run['error'], 'A52 publication fault is a root STOP')
        self.assertIsNone(run['result'], 'A52 publication fault never returns clean success')
        self.assertEqual(len(run['audits']), 1)
        self.assertEqual(run['audits'][0]['verified'], sealed)
        self.assertEqual(sum(event['event'] == 'seal' for event in run['audits'][0]['events']),
                         int(sealed), 'A52 exact owner seal count')
        stops = list(run['ledgers'].glob('split-*/root-stop.json'))
        self.assertEqual(len(stops), 1, 'A52 publication root STOP companion retained')
        stop = json.loads(stops[0].read_bytes())
        self.assertEqual(stop['operation_id'], run['audits'][0]['operation_id'])
        self.assertEqual(stop['fault_type'], type(run['error']).__name__)
        self.assertEqual(stop['sealed'], sealed)
        self.assertTrue(stop['no_clean_success'])
        if staged is not None:
            self.assertEqual(stop['staged'], staged)

    def a52_publication_debt(self, run, published, uncertain, unpublished, current,
                             *, persisted=True):
        error = run['error']
        debt = getattr(error, 'publication_debt', getattr(error, 'debt', None))
        # Non-Exception primaries preserve identity and expose the durable record.
        paths = list(run['ledgers'].glob('split-*/publication-debt.json'))
        if debt is None and paths:
            debt = json.loads(paths[0].read_bytes())
        self.assertIsNotNone(debt, 'A52 exact recovery debt exposed')
        self.assertEqual(debt['published'], published, 'A52 exact published set')
        self.assertEqual(debt['uncertain'], uncertain, 'A52 exact uncertain set')
        self.assertEqual(debt['unpublished'], unpublished, 'A52 exact unpublished set')
        self.assertEqual(debt['current'], current)
        self.assertTrue(debt['recovery_required'])
        self.assertEqual(debt['operation_id'], run['audits'][0]['operation_id'])
        receipts = list(run['ledgers'].glob('*.seal.json'))
        self.assertEqual(len(receipts), 1)
        self.assertEqual(debt['seal_sha256'], hashlib.sha256(receipts[0].read_bytes()).hexdigest())
        self.assertEqual(len(paths), int(persisted), 'A52 exact persisted debt companions')
        if paths:
            self.assertEqual(json.loads(paths[0].read_bytes()), debt)
        self.assertFalse(list(run['ledgers'].glob('split-*/published.json')),
                         'A52 debt has no clean publication record')
        return debt

    def a52_spool_cap_witness(self, root, kind):
        import split_cache
        names = ('AAPL',) if root == 'single' else ('AAPL', 'MSFT')
        item_cap = 5000 if kind == 'item' else split_cache.SPOOL_ITEM_BYTES
        total_cap = (5000 if root == 'single' else 10000) if kind == 'total' else split_cache.SPOOL_TOTAL_BYTES
        overflow, saved = [], []
        original = split_cache._PublicationSpool._save
        def save(path, raw):
            if path.name.startswith('item-'):
                oversized = len(raw) > item_cap or sum(saved) + len(raw) > total_cap
                if oversized:
                    overflow.append(path.name)
                    raise LedgerError('A52 oversized spool-write tripwire')
                saved.append(len(raw))
            return original(path, raw)
        with patch.object(split_cache, 'SPOOL_ITEM_BYTES', item_cap), \
                patch.object(split_cache, 'SPOOL_TOTAL_BYTES', total_cap), \
                patch.object(split_cache._PublicationSpool, '_save', staticmethod(save)):
            run = self.a52_root_exchange(root, names=names)
        self.assertEqual(overflow, [], 'A52 spool cap rejects BEFORE oversized persistence')
        self.assertIsInstance(run['error'], LedgerError)
        self.assertIn('byte limit' if kind == 'item' else 'aggregate limit', str(run['error']))
        expected_saved = 1 if kind == 'total' and root != 'single' else 0
        self.assertEqual(len(saved), expected_saved, 'A52 reached staged spool prefix')
        self.a52_publication_stop(run, sealed=False, staged=list(names[:expected_saved]))
        self.assertEqual(run['old'], run['current'], 'A52 spool cap publishes no sibling')
        self.assertEqual(len(run['scans']), expected_saved + 1)
        return run

    def a52_spool_persistence_witness(self, root, *, after_save):
        import split_cache
        names = ('AAPL',) if root == 'single' else ('AAPL', 'MSFT')
        observed = []
        original = split_cache._PublicationSpool._save
        def save(path, raw):
            if path.name.startswith('item-'):
                observed.append((path, raw))
                if len(observed) == len(names):
                    if after_save:
                        original(path, raw)
                    raise OSError('A52 staged persistence fixture failed')
            return original(path, raw)
        with patch.object(split_cache._PublicationSpool, '_save', staticmethod(save)):
            run = self.a52_root_exchange(root, names=names)
        self.a52_publication_stop(run, sealed=False, staged=list(names[:-1]))
        self.assertIsInstance(run['error'], LedgerError)
        self.assertIsInstance(run['error'].__cause__, OSError)
        self.assertEqual(len(observed), len(names), 'A52 spool persistence fault actually reached')
        path, raw = observed[-1]
        self.assertEqual(path.exists(), after_save, 'A52 failed spool bytes retained, never cleaned')
        if after_save:
            self.assertEqual(path.read_bytes(), raw)
        self.assertEqual(run['old'], run['current'], 'A52 persistence STOP publishes no prefix')

    def a52_spool_custody_witness(self, root, change):
        import split_cache
        names = ('AAPL',) if root == 'single' else ('AAPL', 'MSFT')
        target, reached = names[-1], []
        original = split_cache._PublicationSpool.append
        def append(spool, item, row_index):
            original(spool, item, row_index)
            if item['cache']['ticker'] != target:
                return
            entry = spool.items[-1]
            path = spool.path / entry['name']
            value = json.loads(path.read_bytes())
            if change == 'spool-digest':
                path.write_bytes(path.read_bytes() + b' ')
            else:
                companion, reference = value['companion'], value['reference']
                if change.startswith('companion-'):
                    key = change.removeprefix('companion-')
                    companion[key] = (999 if key in {'resolved_cik', 'configured_cik'}
                                      else 'forged-reference-worker' if key == 'reference_worker'
                                      else 'e' * 64)
                elif change.startswith('reference-'):
                    key = change.removeprefix('reference-')
                    reference[key] = ('FORGED' if key == 'provider_symbol'
                                      else 'forged-operation' if key == 'operation_id'
                                      else 'e' * 64)
                elif change.startswith('source-'):
                    source = next(source for source in value['cache']['sources'].values()
                                  if source.get('provenance') is not None)
                    key = change.removeprefix('source-')
                    if key == 'raw':
                        source['payload_sha256'] = 'e' * 64
                    elif key == 'request':
                        source['provenance']['request_provenance']['requested_symbol'] = 'FORGED'
                    else:
                        source['provenance'][key] = ('forged-operation' if key == 'operation_id'
                                                      else 'e' * 64)
                    # Keep the detached cache checksum consistent: the actual
                    # durable accepted pair must still reject forged provenance.
                    companion['cache_digest'] = digest_value(value['cache'])
                else:
                    self.fail('A52 unknown spool custody fixture ' + change)
                raw = json.dumps(value, sort_keys=True, separators=(',', ':')).encode()
                path.write_bytes(raw)
                entry.update(bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest())
            reached.append(target)
        with patch.object(split_cache._PublicationSpool, 'append', append):
            run = self.a52_root_exchange(root, names=names)
        self.assertEqual(reached, [target], 'A52 actual staged custody forgery reached')
        self.assertEqual(run['old'], run['current'],
                         'A52 ALL companions validated BEFORE first publication')
        self.a52_publication_stop(run, sealed=True, staged=list(names))
        self.assertIsInstance(run['error'], LedgerError)
        self.assertFalse(list(run['ledgers'].glob('split-*/publication-debt.json')),
                         'A52 prepublication rejection is not partial publication')
        self.assertEqual(len(run['scans']), len(names))
        return run

    def a52_publication_cas_witness(self, root, change, *, later=False):
        import split_cache
        names = ('AAPL',) if root == 'single' else ('AAPL', 'MSFT')
        target = names[-1] if later else names[0]
        external = {}
        def race(stage, ticker, bank):
            if stage != 'commit' or ticker != target:
                return
            manifest_path = bank / ticker / 'manifest.json'
            manifest = json.loads(manifest_path.read_bytes())
            if change == 'cache':
                path = split_cache.cache_path(bank, split_cache.manifest_identity(bank, ticker))
                external['cache'] = b'external-cache-race-' + ticker.encode()
                path.write_bytes(external['cache'])
            else:
                if change == 'metadata':
                    manifest['external_note'] = 'fixture CAS race'
                else:
                    manifest[change] = (999 if change in {'conid', 'cik'} else 'FORGED')
                external['manifest'] = json.dumps(manifest).encode()
                manifest_path.write_bytes(external['manifest'])
        run = self.a52_root_exchange(root, names=names, progress=race,
                                     allow_manifest_race=change != 'cache')
        self.assertTrue(external, 'A52 publication CAS race actually reached')
        if 'manifest' in external:
            self.assertEqual(run['current_manifests'][target], external['manifest'],
                             'A52 external manifest change retained exactly')
        self.assertIsInstance(run['error'], LedgerError, 'A52 CAS race is terminal')
        self.assertRegex(str(run['error']), 'publication CAS differs', 'A52 CAS guard reached')
        self.a52_publication_stop(run, sealed=True, staged=list(names))
        prefix = list(names[:-1]) if later else []
        self.a52_publication_debt(run, prefix, [], list(names[len(prefix):]), target)
        for ticker in names:
            expected = external.get('cache', run['old'][ticker]) if ticker == target else run['old'][ticker]
            if ticker in prefix:
                self.assertNotEqual(run['current'][ticker], run['old'][ticker])
            else:
                self.assertEqual(run['current'][ticker], expected, 'A52 CAS preserves external/current bytes')
        return run

    def a52_publication_write_witness(self, root, kind):
        import split_cache
        import fetch_operations
        names = ('AAPL',) if root in {'single'} else ('AAPL', 'MSFT')
        target = names[-1]
        writes = []
        primary = (RequestCancelled('A52 typed publication primary') if kind == 'typed'
                   else KeyboardInterrupt('A52 non-Exception publication primary') if kind == 'base'
                   else OSError('A52 publication fixture failed'))
        def writer(path, raw, original):
            if Path(path).parent.name != split_cache.CACHE_DIR_NAME:
                return original(path, raw)
            writes.append(str(path))
            if len(writes) == len(names):
                if kind in {'after', 'verify'}:
                    original(path, raw)
                if kind == 'verify':
                    Path(path).write_bytes(b'fixture external post-write corruption')
                    return
                raise primary
            return original(path, raw)
        # Successful replacement and then a failed clean-publication record is
        # also STOP, with all items published and recovery still required.
        original_save = split_cache._PublicationSpool._save
        def save(path, raw):
            if kind == 'record' and path.name == 'published.json':
                raise OSError('A52 final publication record failed')
            return original_save(path, raw)
        baseline_operations = set(fetch_operations._OPERATIONS)
        with patch.object(split_cache._PublicationSpool, '_save', staticmethod(save)):
            run = self.a52_root_exchange(root, names=names, writer=None if kind == 'record' else writer)
        self.assertEqual(set(fetch_operations._OPERATIONS), baseline_operations,
                         'A52 failed publication releases owning operation')
        if root == 'main':
            self.assertIsNone(run['error'])
            self.assertEqual(run['result']['cli_exit'], 1, 'A52 actual command STOP exit1')
            self.assertEqual(run['result']['kind'], 'split_cache_command_error')
            debt = run['result']['publication_debt']
            expected_published = list(names) if kind == 'record' else list(names[:-1])
            self.assertEqual(debt['published'], expected_published)
            self.assertEqual(debt['uncertain'], [] if kind == 'record' else [target])
            self.assertEqual(debt['unpublished'], [])
            self.assertTrue(debt['recovery_required'])
            self.assertFalse((run['scratch'] / 'refresh.json').exists(),
                             'A52 failed command does not manufacture success artifact')
            return run
        if kind == 'verify':
            self.assertIsNotNone(run['error'], 'A52 published-byte verification fault is terminal')
        self.a52_publication_stop(run, sealed=True, staged=list(names))
        self.assertIsInstance(run['error'], BaseException)
        if kind in {'typed', 'base'}:
            self.assertIs(run['error'], primary, 'A52 publication preserves typed/non-Exception identity')
        elif kind in {'verify', 'record'}:
            self.assertIsInstance(run['error'], LedgerError)
            self.assertIn('verification differs' if kind == 'verify' else 'evidence persistence failed',
                          str(run['error']))
        else:
            self.assertIsInstance(run['error'], split_cache.PublicationError)
            self.assertIsInstance(run['error'].__cause__, OSError)
        published = list(names) if kind == 'record' else list(names[:-1])
        debt = self.a52_publication_debt(run, published,
            [] if kind == 'record' else [target], [], None if kind == 'record' else target)
        self.assertEqual(debt['fault_type'], 'LedgerError' if kind in {'verify', 'record'} else type(primary).__name__)
        for ticker in names:
            if ticker == target and kind not in {'after', 'verify', 'record'}:
                self.assertEqual(run['current'][ticker], run['old'][ticker])
            else:
                self.assertNotEqual(run['current'][ticker], run['old'][ticker])
        if kind != 'record':
            self.assertEqual(len(writes), len(names), 'A52 actual atomic-writer fault reached')
        return run

    def a52_publication_primary_cleanup_witness(self, root, kind):
        import split_cache
        import fetch_operations
        names = ('AAPL',) if root == 'single' else ('AAPL', 'MSFT')
        primary = (LedgerError('A52 primary ledger STOP') if kind == 'typed'
                   else KeyboardInterrupt('A52 primary non-Exception STOP'))
        writes, secondary = [], {}
        def writer(path, raw, original):
            writes.append(str(path))
            if len(writes) == len(names):
                raise primary
            return original(path, raw)
        original_save = split_cache._PublicationSpool._save
        def save(path, raw):
            if path.name in {'publication-debt.json', 'root-stop.json'}:
                secondary[path.name] = json.loads(raw)
                raise OSError('A52 secondary companion persistence failure')
            return original_save(path, raw)
        original_close = FetchLedger.close
        def close(ledger):
            original_close(ledger)
            secondary['cleanup'] = True
            raise OSError('A52 secondary cleanup fixture failure')
        baseline_operations = set(fetch_operations._OPERATIONS)
        with patch.object(split_cache._PublicationSpool, '_save', staticmethod(save)), \
                patch.object(FetchLedger, 'close', close):
            run = self.a52_root_exchange(root, names=names, writer=writer)
        self.assertIs(run['error'], primary, 'A52 secondary failure never masks primary identity')
        self.assertIsNone(run['result'])
        self.assertEqual(set(fetch_operations._OPERATIONS), baseline_operations,
                         'A52 secondary cleanup still tombstones owning operation')
        self.assertEqual(set(secondary), {'publication-debt.json', 'root-stop.json', 'cleanup'},
                         'A52 all three secondary failure boundaries reached')
        self.assertEqual(primary.__notes__, [
            'split publication debt companion could not be persisted',
            'split root failure companion could not be persisted',
            'split operation cleanup also failed'])
        self.assertTrue(run['audits'][0]['verified'])
        debt = secondary['publication-debt.json']
        self.assertEqual(debt['published'], list(names[:-1]))
        self.assertEqual(debt['uncertain'], [names[-1]])
        self.assertEqual(debt['unpublished'], [])
        self.assertTrue(debt['recovery_required'])
        if kind == 'typed':
            self.assertEqual(primary.publication_debt, debt)
        self.assertFalse(list(run['ledgers'].glob('split-*/publication-debt.json')))
        self.assertFalse(list(run['ledgers'].glob('split-*/root-stop.json')))
        self.assertFalse(list(run['ledgers'].glob('split-*/published.json')))
        for ticker in names:
            if ticker == names[-1]:
                self.assertEqual(run['current'][ticker], run['old'][ticker])
            else:
                self.assertNotEqual(run['current'][ticker], run['old'][ticker])
        return run

    def a52_publication_receipt_primary_witness(self, root, kind):
        import split_cache
        names = ('AAPL',) if root == 'single' else ('AAPL', 'MSFT')
        primary = (LedgerError('A52 primary before recovery receipt') if kind == 'typed'
                   else KeyboardInterrupt('A52 non-Exception before recovery receipt'))
        writes, late_reads = [], []
        def writer(path, raw, original):
            writes.append(str(path))
            if len(writes) == len(names):
                raise primary
            return original(path, raw)
        original_read = split_cache._bounded_read
        def read(path, maximum, *, missing=False):
            if len(writes) == len(names) and str(path).endswith('.seal.json'):
                late_reads.append(str(path))
                raise OSError('A52 secondary receipt read failed')
            return original_read(path, maximum, missing=missing)
        with patch.object(split_cache, '_bounded_read', read):
            run = self.a52_root_exchange(root, names=names, writer=writer)
        self.assertIs(run['error'], primary,
                      'A52 failed recovery receipt read never masks primary identity')
        self.assertEqual(late_reads, [], 'A52 verified receipt digest captured before publication')
        self.a52_publication_stop(run, sealed=True, staged=list(names))
        self.a52_publication_debt(run, list(names[:-1]), [names[-1]], [], names[-1])
        self.assertEqual(len(writes), len(names))
        return run

    def a52_publication_early_receipt_fault_witness(self, root):
        import split_cache
        names = ('AAPL',) if root == 'single' else ('AAPL', 'MSFT')
        primary = OSError('A52 prepublication receipt read failed')
        reads, writes = [], []
        original = split_cache._bounded_read
        def read(path, maximum, *, missing=False):
            if str(path).endswith('.seal.json'):
                reads.append(str(path))
                raise primary
            return original(path, maximum, missing=missing)
        def writer(path, raw, original):
            writes.append(str(path))
            return original(path, raw)
        with patch.object(split_cache, '_bounded_read', read):
            run = self.a52_root_exchange(root, names=names, writer=writer)
        self.assertEqual(len(reads), 1, 'A52 early receipt read fault actually reached')
        self.assertEqual(writes, [], 'A52 receipt capture fails BEFORE any bank write')
        self.assertEqual(run['old'], run['current'])
        self.assertFalse(list(run['ledgers'].glob('split-*/publication-debt.json')))
        if root == 'main':
            self.assertIsNone(run['error'])
            self.assertEqual(run['result']['cli_exit'], 1)
            self.assertEqual(run['result']['kind'], 'split_cache_command_error')
            self.assertIn('prepublication receipt read failed', run['result']['error'])
        else:
            self.assertIs(run['error'], primary)
            self.a52_publication_stop(run, sealed=True, staged=list(names))
        return run

    def test_a52_split_publication_order_and_debt(self):
        def malformed_first(parsed, _count):
            if parsed.netloc == 'query1.finance.yahoo.com' and parsed.path.endswith('/AAPL'):
                return b'{malformed'
        run = self.a52_root_exchange('batch', names=('AAPL', 'MSFT'), body_for=malformed_first)
        self.assertIsNone(run['error'])
        self.assertEqual([row['status'] for row in run['result']['rows']], ['error', 'refreshed'])
        self.assertEqual(run['result']['failures'], 1)
        self.assertEqual(run['old']['AAPL'], run['current']['AAPL'])
        self.assertNotEqual(run['old']['MSFT'], run['current']['MSFT'])
        self.assertEqual(sum(event['event'] == 'seal' for event in run['audits'][0]['events']), 1)
        def terminal_second(_root, ticker, *, ref):
            if ticker == 'MSFT':
                raise LedgerError('A52 typed scan stop')
            return dict(ticker=ticker, verdict='CLEAN', candidates=[],
                        series_fingerprint=hashlib.sha256(b'').hexdigest())
        run = self.a52_root_exchange('batch', names=('AAPL', 'MSFT'), scan=terminal_second)
        self.assertIsInstance(run['error'], LedgerError)
        self.assertEqual(run['old'], run['current'], 'A52 STOP publishes no staged sibling')
        self.assertFalse(run['audits'][0]['verified'])
        writes = []
        def write_failure(path, raw, original):
            writes.append(Path(path).name)
            if len(writes) == 2:
                raise OSError('A52 second publication failed')
            return original(path, raw)
        run = self.a52_root_exchange('batch', names=('AAPL', 'MSFT'), writer=write_failure)
        self.assertIsInstance(run['error'], LedgerError)
        debt = run['error'].debt
        self.assertEqual(debt['published'], ['AAPL'])
        self.assertEqual(debt['uncertain'], ['MSFT'])
        self.assertEqual(debt['unpublished'], [])
        self.assertNotEqual(run['old']['AAPL'], run['current']['AAPL'])
        self.assertEqual(run['old']['MSFT'], run['current']['MSFT'])
        self.assertTrue(run['audits'][0]['verified'])
        self.assertTrue(list(run['ledgers'].glob('split-*/publication-debt.json')))

        import split_cache
        self.assertEqual((split_cache.SPOOL_ITEM_BYTES, split_cache.SPOOL_TOTAL_BYTES),
                         (32 * 1024 * 1024, 128 * 1024 * 1024), 'A52 production spool caps pinned')
        changes = ('spool-digest', 'companion-cache_digest', 'companion-authority_fingerprint',
            'companion-table_digest', 'companion-resolved_cik', 'companion-configured_cik',
            'companion-series_fingerprint', 'companion-reference_worker',
            'reference-provider_symbol', 'reference-operation_id', 'reference-authority_fingerprint',
            'reference-table_digest', 'reference-source_bytes_sha256', 'reference-accepted_digest',
            'source-operation_id', 'source-raw', 'source-accepted_digest', 'source-request')
        for root in ('single', 'batch', 'cli'):
            for kind in ('item', 'total'):
                with self.subTest(spool_cap=kind, root=root):
                    self.a52_spool_cap_witness(root, kind)
            for after_save in (False, True):
                with self.subTest(spool_persistence_after_save=after_save, root=root):
                    self.a52_spool_persistence_witness(root, after_save=after_save)
            for change in changes:
                with self.subTest(spool_custody=change, root=root):
                    self.a52_spool_custody_witness(root, change)
            for change in ('metadata', 'conid', 'symbol', 'provider_symbol', 'cik', 'cache'):
                for later in ((False,) if root == 'single' else (False, True)):
                    with self.subTest(cas=change, later=later, root=root):
                        self.a52_publication_cas_witness(root, change, later=later)
            for kind in ('before', 'after', 'verify', 'record', 'typed', 'base'):
                with self.subTest(atomic_publication=kind, root=root):
                    self.a52_publication_write_witness(root, kind)
            for kind in ('typed', 'base'):
                with self.subTest(primary_and_secondary_faults=kind, root=root):
                    self.a52_publication_primary_cleanup_witness(root, kind)
                with self.subTest(recovery_receipt_fault=kind, root=root):
                    self.a52_publication_receipt_primary_witness(root, kind)
            with self.subTest(early_receipt_read_fault=root):
                self.a52_publication_early_receipt_fault_witness(root)
        for kind in ('before', 'after', 'verify', 'record', 'typed'):
            with self.subTest(actual_command_stop=kind):
                self.a52_publication_write_witness('main', kind)
        self.a52_publication_early_receipt_fault_witness('main')

        for kind, function, old, new in (
                ('item', split_cache._bounded_json, 'if len(encoded) + len(block) > maximum:', 'if False:'),
                ('total', split_cache._PublicationSpool.append,
                 'if self.total_bytes + len(raw) > SPOOL_TOTAL_BYTES:', 'if False:')):
            inverse = _source_inverse(function, old, new)
            owner = split_cache if kind == 'item' else split_cache._PublicationSpool
            with patch.object(owner, function.__name__, inverse):
                for root in ('single', 'batch', 'cli'):
                    with self.subTest(spool_cap_inverse=kind, root=root):
                        with self.assertRaisesRegex(AssertionError, 'A52 spool cap rejects BEFORE oversized persistence'):
                            self.a52_spool_cap_witness(root, kind)
                        print('A52_PUBLICATION_INVERSE_RED', root, 'spool-' + kind)
        inverse = _source_inverse(split_cache._publish_spool,
            '_verify_item(spool.load(entry), context, audit, indexes)',
            'pass  # inverse: skip all-before-first-write verification only')
        with patch.object(split_cache, '_publish_spool', inverse):
            for root in ('batch', 'cli'):
                with self.subTest(all_before_first_write_inverse=root):
                    with self.assertRaisesRegex(AssertionError, 'A52 ALL companions validated BEFORE first publication'):
                        self.a52_spool_custody_witness(root, 'companion-authority_fingerprint')
                    print('A52_PUBLICATION_INVERSE_RED', root, 'all-companions-before-first-write')
        for change, old, new in (
                ('metadata', 'digest != companion["manifest_sha256"] or ', ''),
                ('cache', '!= companion["old_cache_sha256"]',
                 '!= _bytes_digest(_bounded_read(path, SPOOL_ITEM_BYTES, missing=True))')):
            inverse = _source_inverse(split_cache._publish_spool, old, new)
            with patch.object(split_cache, '_publish_spool', inverse):
                for root in ('single', 'batch', 'cli'):
                    with self.subTest(cas_inverse=change, root=root):
                        with self.assertRaisesRegex(AssertionError, 'A52 CAS race is terminal'):
                            self.a52_publication_cas_witness(root, change)
                        print('A52_PUBLICATION_INVERSE_RED', root, 'CAS-' + change)
        for kind, old, new, oracle in (
                ('verify', 'if _bounded_read(path, SPOOL_ITEM_BYTES) != raw:', 'if False:',
                 'A52 published-byte verification fault is terminal'),
                ('before', 'uncertain.append(current)',
                 'uncertain.append(current) if current != spool.items[-1]["ticker"] else None',
                 'A52 exact uncertain set')):
            inverse = _source_inverse(split_cache._publish_spool, old, new)
            with patch.object(split_cache, '_publish_spool', inverse):
                for root in ('single', 'batch', 'cli'):
                    with self.subTest(postseal_inverse=kind, root=root):
                        with self.assertRaisesRegex(AssertionError, oracle):
                            self.a52_publication_write_witness(root, kind)
                        print('A52_PUBLICATION_INVERSE_RED', root, 'postseal-' + kind)
        inverse = _source_inverse(split_cache._publish_spool, 'seal_sha256=seal_sha256,',
            'seal_sha256=_bytes_digest(_bounded_read(context.ledger.receipt_path, OPTIONS_BYTES)),')
        with patch.object(split_cache, '_publish_spool', inverse):
            for root in ('single', 'batch', 'cli'):
                with self.subTest(recovery_receipt_inverse=root):
                    with self.assertRaisesRegex(AssertionError,
                            'A52 failed recovery receipt read never masks primary identity'):
                        self.a52_publication_receipt_primary_witness(root, 'typed')
                    print('A52_PUBLICATION_INVERSE_RED', root, 'recovery-receipt-primary')
        for name, note, witness in (
                ('_publish_spool', 'split publication debt companion could not be persisted', 'debt-primary'),
                ('_run_split', 'split root failure companion could not be persisted', 'root-stop-primary'),
                ('_close_split', 'split operation cleanup also failed', 'cleanup-primary')):
            variable = 'primary' if name == '_close_split' else 'error'
            inverse = _source_inverse(getattr(split_cache, name),
                'BaseException.add_note(' + variable + ', "' + note + '")', 'raise')
            with patch.object(split_cache, name, inverse):
                for root in ('single', 'batch', 'cli'):
                    with self.subTest(secondary_fault_inverse=witness, root=root):
                        with self.assertRaisesRegex(AssertionError,
                                'A52 secondary failure never masks primary identity'):
                            self.a52_publication_primary_cleanup_witness(root, 'typed')
                        print('A52_PUBLICATION_INVERSE_RED', root, witness)
        print('A52_PUBLICATION_REACHED 135 new positive rows;32 intended inverse rows;3 original controls retained')

    def test_a52_split_pre_cancel_terminal(self):
        import fetch_operations
        import fetch_ibkr_bridge
        class Cancel:
            def __getattribute__(inner, name):
                if name == 'is_set':
                    self.assertIsNone(fetch_ibkr_bridge.current_worker(), 'A52 cancel lookup unprivileged')
                    with self.assertRaisesRegex(AuthorityError, 'observer'):
                        fetch_operations.require_root_admission(object())
                return object.__getattribute__(inner, name)
            def is_set(inner):
                self.assertIsNone(fetch_ibkr_bridge.current_worker(), 'A52 cancel invocation unprivileged')
                return True
        for root in ('single', 'batch', 'cli'):
            with self.subTest(root=root):
                run = self.a52_root_exchange(root, overrides={'cancel': Cancel()})
                self.assertIsInstance(run['error'], RequestCancelled)
                self.assertEqual(run['calls'], [])
                self.assertEqual(run['scans'], [])
                self.assertEqual(run['old'], run['current'])
                self.assertFalse(run['audits'][0]['verified'])

    def test_a52_split_post_history_cancel_terminal(self):
        for root in ('single', 'batch', 'cli'):
            stopped = []
            def cancel_after_facts(parsed, _count):
                if parsed.netloc == 'data.sec.gov':
                    stopped.append(True)
            with self.subTest(root=root):
                run = self.a52_root_exchange(root, body_for=cancel_after_facts,
                    overrides={'cancel': lambda: bool(stopped)})
                self.assertIsInstance(run['error'], RequestCancelled)
                self.assertEqual([host for host, _path in run['calls']],
                                 ['query1.finance.yahoo.com', 'www.sec.gov', 'data.sec.gov'])
                self.assertEqual(run['scans'], [])
                self.assertEqual(run['old'], run['current'])
                self.assertFalse(run['audits'][0]['verified'])
                self.assertEqual(len([event for event in run['audits'][0]['events']
                                     if event['event'] == 'result']), 3)

    def a52_identity_forwarding_witness(self, root):
        def candidate(_root, ticker, *, ref):
            return dict(ticker=ticker, verdict='SEAM',
                series_fingerprint=hashlib.sha256(b'').hexdigest(),
                candidates=[dict(ticker=ticker, date='2014-01-10', boundary='2014-01-10',
                    factor=4.0, deep_cv=0.0009, current_anchor_sound=True, strength=1.38)])
        bindings = ({'provider_symbol': 'AAPL-X', 'cik': 123} if root == 'single'
                    else {'provider_symbols': {'AAPL': 'AAPL-X'},
                          'cik_bindings': {'AAPL': 123}})
        run = self.a52_root_exchange(root, overrides=bindings, scan=candidate)
        reference_decisions = [event for audit in run['audits'] for event in audit['events']
            if event['event'] == 'decision' and event['producer_id'] == 'http.stockanalysis.validation']
        self.assertEqual(len(reference_decisions), 1, 'A52 actual reference attempt forwarding')
        self.assertEqual(reference_decisions[0]['payload']['requested']['subject'],
                         'AAPL-X', 'A52 resolved provider identity forwarding')
        self.assertIsNone(run['error'])
        row = run['result'] if root == 'single' else run['result']['rows'][0]
        self.assertEqual(row['status'], 'refreshed')
        self.assertEqual(row['candidate_count'], 1, 'A52 original candidate preserved')
        self.assertEqual(row['cik'], 123)
        self.assertEqual(row['provider_symbol'], 'AAPL-X')
        payload = json.loads(run['current']['AAPL'])
        self.assertEqual(payload['detection']['candidates'][0]['factor'], 4.0)
        return run

    def test_a52_split_candidate_identity_forwarding(self):
        import split_cache
        for root in ('single', 'batch', 'cli'):
            with self.subTest(root=root):
                self.a52_identity_forwarding_witness(root)
        inverse = _source_inverse(split_cache._refresh_ticker_body,
            'validate.fetch_daily_reference(identity["provider_symbol"], rng="Max",',
            'validate.fetch_daily_reference(ticker, rng="Max",')
        with patch.object(split_cache, '_refresh_ticker_body', inverse):
            with self.assertRaisesRegex(AssertionError, 'A52 resolved provider identity forwarding'):
                self.a52_identity_forwarding_witness('batch')
        print('A52_REFERENCE_IDENTITY_INVERSE_RED actual guarded wrong-subject attempt reached')

    def test_a52_split_batch_governor_limit(self):
        import split_cache
        small = self.a52_root_exchange('batch', names=('PACEB', 'PACEA', 'PACEC'),
            overrides={'limit': 2, 'pace_seconds': 0.5,
                       'sleep_fn': lambda *args: self.fail('A52 no legacy local pacing sleep')})
        self.assertIsNone(small['error'])
        self.assertEqual([row['ticker'] for row in small['result']['rows']], ['PACEA', 'PACEB'],
                         'A52 original two-ticker sorted sequential limit retained')
        self.assertEqual(small['result']['processed'], 2)
        self.assertEqual(small['current']['PACEC'], small['old']['PACEC'])
        domains = {}
        for host, stamp in small['call_times']:
            key = 'sec.gov' if host in {'www.sec.gov', 'data.sec.gov'} else host
            domains.setdefault(key, []).append(stamp)
        for host, stamps in domains.items():
            for first, second in zip(stamps, stamps[1:]):
                self.assertGreaterEqual(second - first, 1.2 if host == 'stockanalysis.com' else 1.0,
                                        'A52 governed turn spacing replaces local sleep')
        names = tuple('T' + str(index).zfill(4) for index in range(1100))
        run = self.a52_root_exchange('batch', names=names,
            overrides={'limit': 1000, 'pace_seconds': 0.001,
                       'sleep_fn': lambda *args: self.fail('A52 no local sleep pacing')})
        self.assertIsNone(run['error'], 'A52 actual 1000-ticker root completes')
        result = run['result']
        self.assertEqual((result['requested'], result['processed'], result['limit']), (1100, 1000, 1000))
        self.assertTrue(result['truncated'])
        self.assertEqual(result['failures'], 0)
        self.assertEqual(result['spool_items'], 1000)
        self.assertEqual(len(run['calls']), 3001, 'A52 one map, 1000 of each other required endpoint')
        self.assertEqual(sum(host == 'www.sec.gov' for host, _path in run['calls']), 1,
                         'A52 pending map reused only inside owning operation')
        audit = run['audits'][0]
        self.assertEqual(sum(event['event'] == 'seal' for event in audit['events']), 1)
        decisions = [event for event in audit['events'] if event['event'] == 'decision']
        self.assertEqual(len(decisions), 3001)
        self.assertEqual({tuple(event['payload']['governor_identity']) for event in decisions},
                         {('http', 'yahoo.com', 'public'), ('http', 'sec.gov', 'public'),
                          ('http', 'stockanalysis.com', 'public')})
        for event in decisions:
            policy = event['payload']['governor_policy']
            self.assertGreaterEqual(policy['min_gap_s'], 1.2 if event['producer_id'] ==
                                    'http.stockanalysis.validation' else 1.0)
        paths = sorted(run['ledgers'].glob('split-*/item-*.json'))
        self.assertEqual(len(paths), 1000)
        sizes = [path.stat().st_size for path in paths]
        self.assertEqual(sum(sizes), result['spool_bytes'])
        self.assertLessEqual(max(sizes), split_cache.SPOOL_ITEM_BYTES)
        self.assertLessEqual(sum(sizes), split_cache.SPOOL_TOTAL_BYTES)
        for path in paths:
            item = json.loads(path.read_bytes())
            self.assertEqual(set(item), {'cache', 'reference', 'companion'})
            self.assertNotIn('raw_response_base64', path.read_text())
        for ticker in names[1000:]:
            self.assertEqual(run['current'][ticker], run['old'][ticker], 'A52 truncated items untouched')
        self.assertFalse(split_cache.load_cache(run['root'],
            split_cache.manifest_identity(run['root'], names[0]))['fetch_verified'],
            'A52 offline shape alone never promotes acquisition evidence')
        print('A52_SIZING measured_items=1000 measured_bytes=' + str(sum(sizes)) +
              ' max_item=' + str(max(sizes)) + ' actual_guarded_opens=3001 retained=' + str(run['scratch']))

    def test_a52_split_cli_refresh_artifact(self):
        import split_cache
        run = self.a52_root_exchange('main', artifact='safe')
        self.assertIsNone(run['error'])
        self.assertEqual(run['result']['cli_exit'], 0)
        artifact = json.loads((run['scratch'] / 'refresh.json').read_bytes())
        self.assertEqual(artifact['rows'][0]['status'], 'refreshed')
        self.assertEqual(artifact['operation_id'], run['audits'][0]['operation_id'])
        self.assertTrue(run['audits'][0]['verified'])
        run = self.a52_root_exchange('main', capability=False, artifact='safe')
        self.assertIsNone(run['error'])
        self.assertEqual(run['result']['cli_exit'], 1)
        self.assertIn('RequestRefused', run['result']['error'])
        self.assertEqual(run['clocks'], [])
        self.assertEqual(run['audits'], [])
        self.assertEqual(run['calls'], [])
        self.assertFalse((run['scratch'] / 'refresh.json').exists(), 'A52 held CLI writes no artifact')
        run = self.a52_root_exchange('cli', artifact='bank')
        self.assertIsInstance(run['error'], RequestRefused)
        self.assertEqual(run['calls'], [])
        self.assertEqual(run['old'], run['current'])
        run = self.a52_root_exchange('cli', overrides={'refresh_bank': True})
        self.assertIsNone(run['error'])
        self.assertEqual(run['result']['processed'], 1, 'A52 bank enumeration is inside admitted root')
        class Bomb:
            def __getattribute__(inner, name):
                raise AssertionError('A52 offline CLI inspected fetch object')
        rendered = io.StringIO()
        with redirect_stdout(rendered):
            exit_code = split_cache.main(['--root', str(run['root']), '--offline', 'AAPL'],
                provider=Bomb(), reference_fetcher=Bomb(), scan_fn=Bomb(),
                _clock=lambda: self.fail('A52 offline CLI sampled operation clock'))
        self.assertEqual(exit_code, 0)
        value = json.loads(rendered.getvalue())
        self.assertFalse(value['network'])
        self.assertFalse(value['rows'][0]['fetch_verified'])


    @staticmethod
    def a64_membership_bytes(wikipedia=False):
        symbols = ['MMM', 'AAPL', 'BRK.B', 'BF.B', 'COIN'] + [f'T{i}' for i in range(500)]
        if wikipedia:
            return ('<table id="constituents">' + ''.join(
                f'<tr><td><a href=x>{symbol}</a></td><td>Co</td></tr>' for symbol in symbols)
                + '</table>').encode()
        return ('Symbol,Security,GICS Sector\n' + '\n'.join(
            f'{symbol},Co {symbol},Tech' for symbol in symbols)).encode()

    def a64_membership_exchange(self, root='current', *, outcome='main', default=False,
                                capability=True, old=None, fault=None, options=None,
                                sender_error=None, before_send=None, scratch=None):
        import sp500
        scratch = (Path(tempfile.mkdtemp(prefix='membership-', dir=self.temp.name))
                   if scratch is None else Path(scratch))
        directory, cache = scratch / 'ledgers', scratch / 'cache.json'
        previous_ledgers = set(directory.glob('*.jsonl'))
        previous_reports = set(directory.glob('*.root.json'))
        if old is not None:
            cache.write_bytes(old)
        calls, clocks, responses = [], [], []
        def clock():
            clocks.append('wall')
            return datetime(2026, 8, 4, 17, tzinfo=NY)
        def response(url):
            calls.append(url)
            if before_send is not None:
                before_send(url, scratch)
            if sender_error is not None:
                raise sender_error
            if '/main/' in url and outcome not in ('main', 'parse'):
                raise OSError('offline main provider unavailable')
            if '/master/' in url and outcome != 'master':
                raise OSError('offline master provider unavailable')
            if 'wikipedia.org' in url and outcome not in ('wiki', 'parse'):
                raise OSError('offline Wikipedia unavailable')
            if '/main/' in url and outcome == 'parse':
                return b'Symbol,Name\nAAPL,Apple'
            return self.a64_membership_bytes('wikipedia.org' in url)
        def opened(opener, wire, *, timeout):
            self.assertGreater(timeout, 0)
            self.assertEqual(wire.get_method(), 'GET')
            self.assertEqual(wire.get_header('Accept-encoding'), 'identity')
            stream = io.BytesIO(response(wire.full_url))
            responses.append(stream)
            return stream
        result, error = None, None
        scope = tp.offline_policy() if capability else nullcontext(None)
        with scope as cap:
            args = dict(_test_capability=cap, _evidence_dir=directory,
                _authority=self.authority, _clock=clock, _governors=self.governors, _fault=fault)
            if not default:
                args['_opener'] = response
            args.update(options or {})
            with patch.object(urllib.request.OpenerDirector, 'open', opened):
                try:
                    if root == 'current':
                        result = sp500.current_sp500(cache_path=cache, **args)
                    else:
                        result = sp500.fetch_current_sp500(**args)
                except BaseException as raised:
                    error = raised
        audits = [inspect_ledger(path, require_seal=False) for path in
                  sorted(set(directory.glob('*.jsonl')) - previous_ledgers)]
        reports = [json.loads(path.read_bytes()) for path in
                   sorted(set(directory.glob('*.root.json')) - previous_reports)]
        return dict(result=result, error=error, calls=calls, clocks=clocks, audits=audits,
            reports=reports, directory=directory, cache=cache, old=old,
            current=cache.read_bytes() if cache.exists() else None,
            responses=responses, scratch=scratch)

    def test_a64_membership_main_master_wikipedia_and_parser_fallback(self):
        import sp500
        for root in ('current', 'fetch'):
            for default in (False, True):
                for outcome, expected_source, count in (
                        ('main', 'datahub', 1), ('master', 'datahub', 2),
                        ('wiki', 'wikipedia', 3), ('parse', 'wikipedia', 2)):
                    with self.subTest(root=root, default=default, outcome=outcome):
                        run = self.a64_membership_exchange(root, default=default, outcome=outcome,
                            options={'_today': '1900-01-01'} if root == 'current' else None)
                        self.assertIsNone(run['error'])
                        self.assertEqual(run['result'][1], expected_source)
                        self.assertEqual(run['result'][0], sp500.parse_datahub(self.a64_membership_bytes().decode()))
                        self.assertEqual(len(run['calls']), count)
                        self.assertEqual(run['clocks'], ['wall'])
                        self.assertEqual(len(run['audits']), 1, 'one parent, not two public root mints')
                        audit = run['audits'][0]
                        self.assertTrue(audit['verified'])
                        decisions = [event for event in audit['events'] if event['event'] == 'decision']
                        results = [event for event in audit['events'] if event['event'] == 'result']
                        self.assertEqual(len(decisions), count)
                        self.assertEqual(len(results), count)
                        self.assertEqual(len({event['logical_id'] for event in decisions}), count)
                        self.assertEqual(len({event['attempt_id'] for event in decisions}), count)
                        self.assertTrue(all(event['worker_id'] == 'membership-fetch' for event in decisions))
                        self.assertEqual([event['payload']['http_request']['url'] for event in decisions], run['calls'])
                        self.assertEqual(results[-1]['payload']['membership_count'], len(run['result'][0]))
                        self.assertEqual(results[-1]['payload']['response_digest'], digest_value(run['result'][0]))
                        self.assertEqual(results[-1]['payload']['raw_response_digest'],
                            hashlib.sha256(self.a64_membership_bytes(expected_source == 'wikipedia')).hexdigest())
                        self.assertTrue(all(stream.closed for stream in run['responses']))
                        report = run['reports'][0]
                        self.assertTrue(report['membership_verified'])
                        self.assertTrue(report['ledger_verified'])
                        self.assertEqual(report['state'], 'complete')
                        if root == 'current':
                            self.assertEqual(run['result'][2], '2026-08-04', '_today cannot forge freshness')
                            self.assertTrue(report['published'])
                            payload = json.loads(run['current'])
                            self.assertEqual(payload['tickers'], run['result'][0])
                            self.assertEqual(payload['provenance']['operation_id'], audit['operation_id'])
                            self.assertNotIn(str(run['scratch']).encode(), run['current'], 'no deployment path in cache')
                        else:
                            self.assertIsNone(run['current'])

    def test_a64_membership_provider_exhaustion_unverified_cache_and_bundled(self):
        import sp500
        tickers = sp500.parse_datahub(self.a64_membership_bytes().decode())
        legacy = json.dumps(dict(tickers=tickers, asof='2026-06-24', source='datahub')).encode()
        for old, expected in ((legacy, ('cache', '2026-06-24')), (b'{broken', ('bundled', None)),
                              (b'{"tickers":["AAPL"]}', ('bundled', None)), (None, ('bundled', None))):
            with self.subTest(old=old is None):
                run = self.a64_membership_exchange(outcome='exhaust', old=old)
                self.assertIsNone(run['error'])
                self.assertEqual(run['result'][1:], expected)
                self.assertEqual(run['current'], old)
                self.assertEqual(len(run['calls']), 3)
                self.assertTrue(run['audits'][0]['verified'])
                self.assertFalse(run['reports'][0]['membership_verified'])
                self.assertFalse(run['reports'][0]['published'])
                self.assertEqual(run['reports'][0]['state'], 'offline_unverified')
                self.assertEqual(run['result'][0], tickers if expected[0] == 'cache' else sp500.SP500)
        run = self.a64_membership_exchange('fetch', outcome='exhaust')
        self.assertIsInstance(run['error'], sp500.Sp500FetchError)
        self.assertEqual(len(run['calls']), 3)
        self.assertIsNone(run['result'])

    def test_a64_membership_default_roots_hold_before_warm_cache_or_context(self):
        import sp500
        warm = json.dumps(dict(tickers=list(sp500.SP500), asof='2026-08-04', source='datahub')).encode()
        for root in ('current', 'fetch'):
            with self.subTest(root=root), patch.object(FetchRunContext, 'create',
                    side_effect=AssertionError('unactivated root opened context')):
                run = self.a64_membership_exchange(root, capability=False, old=warm)
            self.assertIsInstance(run['error'], RequestRefused)
            self.assertIn('not activated', str(run['error']))
            self.assertEqual(run['calls'], [])
            self.assertEqual(run['clocks'], [])
            self.assertEqual(run['audits'], [])
            self.assertEqual(run['reports'], [])
            self.assertEqual(run['current'], warm)

    def test_a64_membership_terminals_never_fallback_or_replace_cache(self):
        sentinel = b'last-good membership cache bytes'
        for error in (AuthorityError('authority drift'), LedgerError('ledger failed'),
                      RequestCancelled('cancelled'), ResponseQuarantined('quarantined'),
                      URLError(AuthorityError('wrapped authority')),
                      RuntimeError('unexpected non-provider failure')):
            for root in ('current', 'fetch'):
                with self.subTest(root=root, error=type(error).__name__):
                    run = self.a64_membership_exchange(root, old=sentinel, sender_error=error)
                    self.assertIsNone(run['result'])
                    self.assertIsNotNone(run['error'])
                    self.assertEqual(len(run['calls']), 1)
                    self.assertEqual(run['current'], sentinel)
                    self.assertEqual(run['reports'][0]['state'], 'failed')
                    self.assertFalse(run['reports'][0]['published'])
                    self.assertFalse(run['reports'][0]['membership_verified'])

    def test_a64_membership_decision_result_seal_durability_faults(self):
        sentinel = b'last-good cache'
        for event in ('decision', 'result', 'seal'):
            for stage in ('append', 'flush', 'fsync'):
                def fault(actual, position, target=event, phase=stage):
                    if actual == target and position == phase:
                        raise OSError('injected membership journal failure')
                with self.subTest(event=event, stage=stage):
                    run = self.a64_membership_exchange(old=sentinel, fault=fault)
                    self.assertIsInstance(run['error'], LedgerError)
                    self.assertEqual(run['current'], sentinel)
                    self.assertEqual(len(run['calls']), 0 if event == 'decision' else 1)
                    self.assertIsNone(run['result'])
                    self.assertFalse(run['reports'][0]['published'])
                    self.assertEqual(run['reports'][0]['state'], 'failed')

    def test_a64_membership_join_cleanup_preserves_primary_and_last_good(self):
        import fetch_operations as operations
        sentinel = b'last-good before child join cleanup'
        for root in ('current', 'fetch'):
            for selected in ('membership-fetch', 'membership-root'):
                for primary in (None, AuthorityError('primary membership authority failure'),
                                RequestCancelled('primary membership cancellation')):
                    closed = []
                    with self.subTest(root=root, worker=selected, primary=type(primary).__name__), ExitStack() as patches:
                        def install(_url, _scratch):
                            # Observe an already-created child, never import or
                            # construct the restricted mint types in this owner.
                            children = [child for child, state in operations._CHILDREN.items()
                                        if state.worker_id == selected]
                            self.assertEqual(len(children), 1)
                            child_type = type(children[0])
                            original_close = child_type.close
                            def fault(child):
                                worker_id = operations._CHILDREN[child].worker_id
                                original_close(child)
                                if worker_id == selected:
                                    closed.append(worker_id)
                                    raise LedgerError('injected drained-child cleanup fault')
                            patches.enter_context(patch.object(child_type, 'close', fault))
                        run = self.a64_membership_exchange(root, old=sentinel, sender_error=primary,
                                                          before_send=install)
                        self.assertTrue(closed)
                        self.assertIsNone(run['result'])
                        self.assertEqual(len(run['calls']), 1)
                        self.assertEqual(run['current'], sentinel)
                        if primary is None:
                            self.assertIsInstance(run['error'], LedgerError)
                        else:
                            self.assertIs(run['error'], primary, 'joined-child cleanup cannot replace primary terminal')
                        self.assertEqual(run['reports'][0]['state'], 'failed')
                        self.assertFalse(run['reports'][0]['published'])
                        self.assertFalse(run['audits'][0]['verified'])
                    self.assertEqual(operations._OPERATIONS, {})
                    self.assertEqual(operations._CHILDREN, {})

    def test_a64_membership_unjoined_cleanup_never_delivers_or_publishes(self):
        import fetch_operations as operations
        for root in ('current', 'fetch'):
            for primary in (None, RequestCancelled('primary before unjoined cleanup')):
                baseline_children, baseline_operations = set(operations._CHILDREN), set(operations._OPERATIONS)
                reached = []
                try:
                    with self.subTest(root=root, primary=type(primary).__name__), ExitStack() as patches:
                        def install(_url, _scratch):
                            child = next(child for child, state in operations._CHILDREN.items()
                                         if state.worker_id == 'membership-fetch')
                            child_type, original_close = type(child), type(child).close
                            def fault(value):
                                if operations._CHILDREN[value].worker_id == 'membership-fetch':
                                    reached.append(value)
                                    raise LedgerError('injected before-drain child cleanup fault')
                                return original_close(value)
                            patches.enter_context(patch.object(child_type, 'close', fault))
                        run = self.a64_membership_exchange(root, old=b'last-good unjoined child',
                                                          sender_error=primary, before_send=install)
                        self.assertTrue(reached)
                        self.assertIsNone(run['result'])
                        self.assertEqual(run['current'], run['old'])
                        self.assertIsInstance(run['error'], LedgerError if primary is None else RequestCancelled)
                        if primary is not None:
                            self.assertIs(run['error'], primary)
                        self.assertEqual(run['reports'][0]['state'], 'failed')
                        self.assertFalse(run['reports'][0]['published'])
                        self.assertFalse(run['audits'][0]['verified'])
                        self.assertTrue(set(operations._CHILDREN) - baseline_children,
                                        'retain actual unjoined custody; never hide it with a registry reset')
                finally:
                    # Restore real close first, then join only the handles made
                    # by this fixture. No unrelated context or registry reset.
                    pending = set(operations._CHILDREN) - baseline_children
                    while pending:
                        ready = [child for child in pending if not operations._CHILDREN[child].children]
                        self.assertTrue(ready)
                        for child in ready:
                            child.close()
                            pending.remove(child)
                    for handle in set(operations._OPERATIONS) - baseline_operations:
                        handle.close()
                    self.assertEqual(set(operations._CHILDREN), baseline_children)
                    self.assertEqual(set(operations._OPERATIONS), baseline_operations)

    def test_a64_membership_cache_cannot_overwrite_prior_or_nested_evidence(self):
        import sp500
        directory = Path(self.temp.name) / 'evidence-path-guard'
        directory.mkdir()
        for suffix in ('prior.jsonl', 'prior.seal.json', 'nested/retained.root.json'):
            cache = directory / suffix
            cache.parent.mkdir(parents=True, exist_ok=True)
            cache.write_bytes(b'last-good retained evidence')
            with self.subTest(path=suffix), tp.offline_policy() as cap:
                sent = []
                with self.assertRaisesRegex(RequestRefused, 'overwrite operation evidence'):
                    sp500.current_sp500(cache_path=cache, _test_capability=cap,
                        _evidence_dir=directory, _authority=self.authority,
                        _clock=lambda: datetime(2026, 8, 4, 17, tzinfo=NY),
                        _governors=self.governors,
                        _opener=lambda url: sent.append(url) or self.a64_membership_bytes())
                self.assertEqual(sent, [])
                self.assertEqual(cache.read_bytes(), b'last-good retained evidence')

    def test_a64_membership_receipt_and_result_release_authority_faults(self):
        sentinel = b'last-good before receipt/release fault'
        factory = FetchRunContext.create
        cases = [('receipt', phase, 'receipt') for phase in ('append', 'flush', 'fsync', 'publish')]
        cases += [('decision', 'fsync', 'policy'), ('result', 'fsync', 'policy'),
                  ('decision', 'fsync', 'cancel'), ('result', 'fsync', 'cancel'),
                  ('result', 'fsync', 'authority')]
        for root in ('current', 'fetch'):
            for event, stage, kind in cases:
                contexts, faults, cancel = [], [], threading.Event()
                def created(*args, **kwargs):
                    context = factory(*args, **kwargs)
                    contexts.append(context)
                    return context
                def fault(actual, position):
                    if actual != event or position != stage:
                        return
                    faults.append((actual, position))
                    if kind == 'receipt':
                        raise OSError('injected membership receipt failure')
                    if kind == 'policy':
                        contexts[0].test_capability._live = False
                    if kind == 'cancel':
                        cancel.set()
                    if kind == 'authority':
                        object.__setattr__(contexts[0], 'authority',
                                           replace(contexts[0].authority, fingerprint='0' * 64))
                with self.subTest(root=root, event=event, stage=stage, kind=kind), patch.object(
                        FetchRunContext, 'create', side_effect=created):
                    run = self.a64_membership_exchange(root, old=sentinel, fault=fault, options={'_cancel': cancel})
                    self.assertEqual(len(contexts), 1)
                    self.assertTrue(faults)
                    self.assertIsInstance(run['error'], LedgerError if kind == 'receipt' else
                                          RequestCancelled if kind == 'cancel' else AuthorityError)
                    self.assertIsNone(run['result'])
                    self.assertEqual(run['current'], sentinel)
                    self.assertEqual(len(run['calls']), 0 if event == 'decision' else 1)
                    self.assertEqual(run['reports'][0]['state'], 'failed')
                    self.assertFalse(run['reports'][0]['published'])
                    self.assertFalse(run['reports'][0]['membership_verified'])
                    self.assertTrue(contexts[0].ledger._file.closed)

    def test_a64_membership_transport_callback_has_no_borrowed_root_authority(self):
        import sp500
        def foreign(url, scratch):
            with self.assertRaisesRegex(AuthorityError, 'observer'):
                sp500.fetch_current_sp500(_test_capability=tp._Policy(tp._ACTIVE))
        run = self.a64_membership_exchange(before_send=foreign)
        self.assertIsNone(run['error'])
        self.assertEqual(len(run['calls']), 1)

    def test_a64_membership_cleanup_precedes_cache_publication(self):
        import sp500
        import stock_storage
        import fetch_operations as fops
        sentinel = b'last-good cache before cleanup'
        original_close = FetchLedger.close
        original_write = stock_storage._atomic_write_bytes
        trace = []
        def closed(ledger):
            original_close(ledger)
            trace.append('ledger_closed')
        def written(path, raw):
            if Path(path).name == 'cache.json':
                self.assertEqual(trace, ['ledger_closed'])
                self.assertEqual(fops._OPERATIONS, {}, 'publication cannot retain root authority')
                self.assertEqual(fops._CHILDREN, {})
                trace.append('cache_write')
            return original_write(path, raw)
        with patch.object(FetchLedger, 'close', closed), patch.object(stock_storage, '_atomic_write_bytes', written):
            positive = self.a64_membership_exchange(old=sentinel)
        self.assertIsNone(positive['error'])
        self.assertEqual(trace, ['ledger_closed', 'cache_write'])
        for root in ('current', 'fetch'):
            def failed_close(ledger):
                original_close(ledger)
                raise OSError('injected closed-stream cleanup fault')
            with self.subTest(root=root), patch.object(FetchLedger, 'close', failed_close):
                failed = self.a64_membership_exchange(root, old=sentinel)
            self.assertIsInstance(failed['error'], LedgerError)
            self.assertIsNone(failed['result'])
            self.assertEqual(failed['current'], sentinel)
            self.assertEqual(len(failed['calls']), 1)
            self.assertEqual(failed['reports'][0]['state'], 'failed')
            self.assertFalse(failed['reports'][0]['published'])
            self.assertEqual(fops._OPERATIONS, {})
            self.assertEqual(fops._CHILDREN, {})

    def test_a64_membership_option_and_pre_read_failures_have_companion(self):
        import sp500
        sentinel = b'last-good cache on preflight failure'
        for options in ({'timeout': 0}, {'timeout': True}, {'_budget_seconds': 0},
                        {'_budget_seconds': float('inf')}):
            with self.subTest(options=options):
                run = self.a64_membership_exchange(old=sentinel, options=options)
            self.assertIsInstance(run['error'], AuthorityError)
            self.assertEqual(run['calls'], [])
            self.assertEqual(run['current'], sentinel)
            self.assertIsNone(run['result'])
            self.assertEqual(run['reports'][0]['state'], 'failed')
        read = sp500._read
        def unreadable(path, maximum, *, missing=False):
            if Path(path).name == 'cache.json':
                raise OSError('injected cache pre-read failure')
            return read(path, maximum, missing=missing)
        with patch.object(sp500, '_read', unreadable):
            run = self.a64_membership_exchange(old=sentinel)
        self.assertIsInstance(run['error'], LedgerError)
        self.assertEqual(run['current'], sentinel)
        self.assertEqual(run['calls'], [])
        self.assertEqual(run['reports'][0]['state'], 'failed')

    def test_a64_membership_post_replace_and_final_report_faults_restore_last_good(self):
        import stock_storage
        original = stock_storage._atomic_write_bytes
        for stage in ('replace-then-raise', 'final-companion'):
            for old in (b'last-good-before-final-companion', None):
                committed, injected = [], []
                def failed(path, raw):
                    is_cache = Path(path).name == 'cache.json'
                    is_report = Path(path).name.endswith('.root.json')
                    if is_cache:
                        original(path, raw)
                        committed.append(raw)
                        if stage == 'replace-then-raise' and not injected:
                            injected.append(True)
                            raise OSError('membership replace completed before injected failure')
                        return
                    if is_report and committed and stage == 'final-companion' and not injected:
                        injected.append(True)
                        raise OSError('membership final companion failed after cache replacement')
                    return original(path, raw)
                with self.subTest(stage=stage, previous=old is not None), patch.object(
                        stock_storage, '_atomic_write_bytes', failed):
                    run = self.a64_membership_exchange(old=old)
                    self.assertIsInstance(run['error'], LedgerError)
                    self.assertIsNone(run['result'])
                    self.assertEqual(len(run['calls']), 1)
                    self.assertTrue(committed and injected)
                    self.assertEqual(run['current'], old, 'failed final publication must restore exact expected-old bytes')
                    self.assertEqual(run['reports'][0]['state'], 'failed')
                    self.assertFalse(run['reports'][0]['published'])
                    self.assertTrue(run['reports'][0]['publication_rolled_back'])
                    self.assertFalse(run['reports'][0]['recovery_required'])

    def test_a64_membership_companion_and_cache_replace_failures_are_terminal(self):
        import stock_storage
        sentinel = b'last-good cache on publication failure'
        original = stock_storage._atomic_write_bytes
        for target in ('companion', 'cache'):
            def failed(path, raw):
                hit = (Path(path).name.endswith('.root.json') if target == 'companion'
                       else Path(path).name == 'cache.json')
                if hit:
                    raise OSError('injected atomic persistence failure')
                return original(path, raw)
            with self.subTest(target=target), patch.object(stock_storage, '_atomic_write_bytes', failed):
                run = self.a64_membership_exchange(old=sentinel)
            self.assertIsInstance(run['error'], LedgerError)
            self.assertIsNone(run['result'])
            self.assertEqual(run['current'], sentinel)
            self.assertEqual(len(run['calls']), 1)
            if target == 'companion':
                self.assertEqual(run['reports'], [], 'missing companion is never delivery proof')
            else:
                self.assertEqual(run['reports'][0]['state'], 'failed')
                self.assertFalse(run['reports'][0]['published'])
                self.assertFalse(run['reports'][0]['recovery_required'],
                                 'pre-replace failure has verified unchanged bytes, not uncertain recovery debt')
                self.assertFalse(run['reports'][0]['publication_rolled_back'])

    def test_a64_membership_competing_writer_is_preserved(self):
        import sp500
        original = sp500._save
        sentinel, competing = b'last-good expected-old cache', b'concurrent cache writer'
        def changed(path, report):
            original(path, report)
            if report['state'] == 'accepted_unpublished':
                (Path(path).parent.parent / 'cache.json').write_bytes(competing)
        with patch.object(sp500, '_save', changed):
            run = self.a64_membership_exchange(old=sentinel)
        self.assertIsInstance(run['error'], LedgerError)
        self.assertIn('CAS', str(run['error']))
        self.assertIsNone(run['result'])
        self.assertEqual(run['current'], competing)
        self.assertEqual(len(run['calls']), 1)
        self.assertFalse(run['reports'][0]['published'])

    def test_a64_membership_rollback_preserves_late_writer_and_exposes_recovery_debt(self):
        import stock_storage
        original = stock_storage._atomic_write_bytes
        sentinel, competing = b'last-good for rollback', b'late independent cache writer'
        for kind in ('late-writer', 'rollback-failure'):
            cache_paths, final_faults, rollbacks = [], [], []
            def failed(path, raw):
                if Path(path).name == 'cache.json':
                    if raw == sentinel and cache_paths:
                        rollbacks.append(True)
                        if kind == 'rollback-failure':
                            raise OSError('injected rollback persistence failure')
                    original(path, raw)
                    cache_paths.append(Path(path))
                    return
                if Path(path).name.endswith('.root.json') and cache_paths and not final_faults:
                    final_faults.append(True)
                    if kind == 'late-writer':
                        original(cache_paths[0], competing)
                    raise OSError('injected final companion failure')
                return original(path, raw)
            with self.subTest(kind=kind), patch.object(stock_storage, '_atomic_write_bytes', failed):
                run = self.a64_membership_exchange(old=sentinel)
            self.assertIsInstance(run['error'], LedgerError)
            self.assertIsNone(run['result'])
            self.assertEqual(len(run['calls']), 1)
            self.assertEqual(run['reports'][0]['state'], 'failed')
            self.assertTrue(run['reports'][0]['recovery_required'])
            self.assertTrue(run['reports'][0]['publication_uncertain'])
            self.assertIn('rollback_fault_type', run['reports'][0])
            if kind == 'late-writer':
                self.assertEqual(run['current'], competing, 'rollback must never overwrite a foreign writer')
                self.assertEqual(rollbacks, [])
            else:
                self.assertTrue(rollbacks)
                self.assertEqual(json.loads(run['current'])['kind'], 'sp500_membership')
                self.assertNotEqual(run['current'], sentinel)

    def test_a64_membership_post_publication_policy_expiry_restores_last_good(self):
        import stock_storage
        import sp500
        import fetch_operations as operations
        original_write, original_save = stock_storage._atomic_write_bytes, sp500._save
        policies, replacements = [], []
        def observed(path, report):
            if report['state'] == 'accepted_unpublished':
                policies.extend(run.context.test_capability for run in operations._OPERATIONS.values())
            return original_save(path, report)
        def expired(path, raw):
            original_write(path, raw)
            if Path(path).name == 'cache.json' and not replacements:
                replacements.append(True)
                self.assertEqual(len(policies), 1)
                policies[0]._live = False
        sentinel = b'last-good before publication policy expiry'
        with patch.object(sp500, '_save', observed), patch.object(stock_storage, '_atomic_write_bytes', expired):
            run = self.a64_membership_exchange(old=sentinel)
        self.assertIsInstance(run['error'], AuthorityError)
        self.assertIsNone(run['result'])
        self.assertEqual(run['current'], sentinel)
        self.assertEqual(len(run['calls']), 1)
        self.assertEqual(run['reports'][0]['state'], 'failed')
        self.assertTrue(run['reports'][0]['publication_rolled_back'])
        self.assertFalse(run['reports'][0]['published'])
        self.assertFalse(run['reports'][0]['recovery_required'])

    def test_a64_membership_invalid_legacy_date_has_no_freshness(self):
        import sp500
        legacy = json.dumps(dict(tickers=list(sp500.SP500), asof='2026-99-99')).encode()
        run = self.a64_membership_exchange(old=legacy, outcome='exhaust')
        self.assertIsNone(run['error'])
        self.assertEqual(run['result'][1:], ('cache', None))
        self.assertEqual(run['current'], legacy)
        self.assertFalse(run['reports'][0]['membership_verified'])
        self.assertFalse(run['reports'][0]['prior_evidence_verified'])

    def test_a64_membership_verified_cache_relocation_and_new_context(self):
        first = self.a64_membership_exchange()
        self.assertIsNone(first['error'])
        for moved in (False, True):
            with self.subTest(moved=moved):
                scratch = first['scratch']
                if moved:
                    scratch = Path(self.temp.name) / 'copied membership with spaces'
                    shutil.copytree(first['scratch'], scratch)
                old = first['current']
                run = self.a64_membership_exchange(outcome='exhaust', old=old, scratch=scratch)
                self.assertIsNone(run['error'])
                self.assertEqual(run['result'], first['result'][:1] + ('cache', '2026-08-04'))
                self.assertEqual(run['current'], old)
                self.assertTrue(run['reports'][0]['prior_evidence_verified'],
                                'new context verifies exact portable producing seal')
                self.assertFalse(run['reports'][0]['membership_verified'],
                                 'prior evidence is not a fresh provider fetch')
                self.assertFalse(run['reports'][0]['published'])
                self.assertNotEqual(run['audits'][0]['operation_id'], first['audits'][0]['operation_id'])
                self.assertEqual(len(run['calls']), 3)
                self.assertEqual(run['clocks'], ['wall'])

    def test_a64_membership_cache_provenance_tampering_has_no_freshness(self):
        first = self.a64_membership_exchange()
        self.assertIsNone(first['error'])
        payload = json.loads(first['current'])
        def mutate(path, value):
            changed = deepcopy(payload)
            owner = changed
            for key in path[:-1]:
                owner = owner[key]
            owner[path[-1]] = value
            return json.dumps(changed).encode()
        cases = (
            (('tickers',), sorted(payload['tickers'][:-1] + ['ZZZZZZ'])),
            (('source',), 'wikipedia'), (('asof',), '2099-01-01'),
            (('subject',), 'OTHER'), (('version',), True),
            (('provenance', 'accepted_digest'), '0' * 64),
            (('provenance', 'raw_digest'), '0' * 64),
            (('provenance', 'authority_fingerprint'), '0' * 64),
            (('provenance', 'table_digest'), '0' * 64),
            (('provenance', 'consensus_digest'), '0' * 64),
            (('provenance', 'seal_sha256'), '0' * 64),
            (('provenance', 'operation_id'), '0' * 32),
            (('provenance', 'attempt_id'), '0' * 32),
            (('provenance', 'logical_id'), '0' * 32),
            (('provenance', 'worker_id'), 'unrelated-worker'),
            (('provenance', 'ledger_name'), '../outside.jsonl'),
            (('provenance', 'request_provenance', 'branch'), 'master'),
            (('provenance', 'request_provenance', 'endpoint'), 'sp500.wikipedia'),
            (('provenance', 'request_provenance', 'subject'), 'OTHER'),
            (('provenance', 'request_provenance', 'body_cap_bytes'), 1),
            (('provenance', 'request_provenance', 'url'), 'https://example.com/'),
        )
        for index, (path, value) in enumerate(cases):
            with self.subTest(path=path):
                scratch = Path(self.temp.name) / ('tampered-cache-' + str(index))
                shutil.copytree(first['scratch'], scratch)
                old = mutate(path, value)
                run = self.a64_membership_exchange(scratch=scratch, old=old, outcome='exhaust')
                self.assertIsNone(run['error'])
                self.assertEqual(run['current'], old)
                self.assertEqual(run['result'][1], 'cache')
                self.assertFalse(run['reports'][0]['prior_evidence_verified'],
                                 'tampered cache is UNVERIFIED, not current membership')
                self.assertFalse(run['reports'][0]['membership_verified'])
                self.assertEqual(len(run['calls']), 3)
        for kind in ('new-authority', 'new-date', 'missing-producing-seal'):
            with self.subTest(kind=kind):
                scratch = Path(self.temp.name) / kind
                ignored = shutil.ignore_patterns('*.seal.json') if kind == 'missing-producing-seal' else None
                shutil.copytree(first['scratch'], scratch, ignore=ignored)
                options = ({'_authority': replace(self.authority, fingerprint='0' * 64)}
                           if kind == 'new-authority' else
                           {'_clock': lambda: datetime(2026, 8, 5, 17, tzinfo=NY)}
                           if kind == 'new-date' else {})
                run = self.a64_membership_exchange(scratch=scratch, old=first['current'],
                                                   outcome='exhaust', options=options)
                self.assertIsNone(run['error'])
                self.assertEqual(run['current'], first['current'])
                self.assertFalse(run['reports'][0]['prior_evidence_verified'])

    def test_a64_membership_actual_gui_presets_hold_before_legacy_cache(self):
        import sp500
        import fetch_operation_inventory as inventory
        source = (Path(__file__).resolve().parents[1] / 'display_data.py').read_text(encoding='utf-8')
        tree = ast.parse(source)
        app = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == 'DataViewerApp')
        # Exact real caller names are read from the three preset methods below;
        # require all three reach the same owned async dispatch, not a stub root.
        dispatches = [node for node in app.body if isinstance(node, ast.FunctionDef)
                      and any(isinstance(call, ast.Call) and isinstance(call.func, ast.Attribute)
                              and call.func.attr == '_current_sp500_async' for call in ast.walk(node))]
        self.assertEqual(len(dispatches), 3)
        self.assertEqual({node.name for node in dispatches},
                         {'_export_sp500_preset', '_exd_pick_sp500', '_storage_find_sp500'})
        warm = json.dumps(dict(tickers=list(sp500.SP500), asof='2026-08-04')).encode()
        cache = Path(self.temp.name) / 'sp500_current.json'
        cache.write_bytes(warm)
        for preset in dispatches:
            ready, scheduled, launched = [], [], []
            callback = lambda *values: ready.append(values)
            observer = SimpleNamespace(_storage_root=Path(self.temp.name) / 'bank',
                root=SimpleNamespace(after=lambda delay, fn: (scheduled.append(delay), fn())),
                _storage_export_say=lambda message: None, _storage_find_say=lambda message: None,
                _exd_pick_status=SimpleNamespace(set=lambda message: None),
                _export_sp500_apply=callback, _exd_pick_sp500_apply=callback,
                _storage_find_sp500_apply=callback)
            environment = dict(sp500=sp500, Path=Path)
            async_body = self.a52_extracted_dialog_function(source,
                ('DataViewerApp', '_current_sp500_async'), environment)
            observer._current_sp500_async = lambda on_ready: async_body(observer, on_ready)
            body = self.a52_extracted_dialog_function(source, ('DataViewerApp', preset.name), environment)
            def local_thread(*, target, daemon, name):
                self.assertTrue(daemon)
                self.assertEqual(name, 'sp500-fetch')
                return SimpleNamespace(start=lambda: (launched.append(name), target()))
            with (self.subTest(preset=preset.name), patch.object(FetchRunContext, 'create',
                    side_effect=AssertionError('GUI must not mint context')),
                    patch.object(sp500, '_read', side_effect=AssertionError('GUI must not read legacy cache')),
                    patch.object(threading, 'Thread', local_thread)):
                body(observer)
            self.assertEqual(ready, [(list(sp500.SP500), 'bundled', None)])
            self.assertEqual(scheduled, [0])
            self.assertEqual(launched, ['sp500-fetch'])
            self.assertEqual(observer._sp500_live, ready[0])
            self.assertEqual(cache.read_bytes(), warm)
        self.assertIn(('DataViewerApp._current_sp500_async._work', 'current_sp500', 'sp500'),
                      inventory.GUI_HTTP_DISPATCHES)
        self.assertEqual(inventory.DISPATCH_PURPOSES['current_sp500'], 'membership')

    def a64_membership_wire(self, endpoint, raw, *, stage=None, code=200, close_fault=False, cancelled=None):
        """Real director/handler/HTTP parser; only DNS/socket/TLS are memory doubles."""
        import fetch_http_membership as membership
        clock, trace, chunks, deadlines = Clock(), [], [], []
        producer = 'http.sp500.github' if endpoint == 'sp500.github' else 'http.sp500.wikipedia'
        branch = 'main' if endpoint == 'sp500.github' else 'wikipedia'
        host = dict(_A54_TARGETS)[endpoint]
        prefix = (f'HTTP/1.1 {code} fixture\r\nContent-Length: {len(raw)}\r\n'
                  'Location: https://other.invalid/\r\nConnection: close\r\n\r\n').encode()
        class Wire(io.BytesIO):
            header_delayed = False
            body_delayed = False
            def readline(self, *args):
                if stage == 'headers' and not self.header_delayed:
                    self.header_delayed = True
                    clock.sleep(21)
                return super().readline(*args)
            def read1(self, count):
                chunks.append(count)
                if stage == 'body' and not self.body_delayed:
                    self.body_delayed = True
                    clock.sleep(21)
                if stage == 'primary-and-close':
                    raise AuthorityError('primary body failure')
                value = super().read1(count)
                if stage == 'cancel-body':
                    cancelled.set()
                return value
            def close(self):
                trace.append('body-close')
                super().close()
                if close_fault:
                    raise OSError('membership wire cleanup fault')
        wire = Wire(prefix + raw)
        self.addCleanup(io.BytesIO.close, wire)
        class Socket:
            def settimeout(self, value):
                trace.append(('timeout', value))
            def connect(self, address):
                trace.append(('connect', address))
                clock.sleep(21 if stage == 'connect' else 1)
            def sendall(self, value):
                trace.append(('send', value))
            def makefile(self, mode):
                self_outer.assertEqual(mode, 'rb')
                return wire
            def close(self):
                trace.append('socket-close')
        self_outer, sock = self, Socket()
        def lookup(target):
            self.assertEqual((target.endpoint_id, target.host), (endpoint, host))
            trace.append(('dns', target.host))
            if stage == 'dns':
                clock.sleep(21)
            return [(http_transport.socket.AF_INET, http_transport.socket.SOCK_STREAM,
                     http_transport.socket.IPPROTO_TCP, '', ('127.0.0.1', 443))]
        def tls(context, socket, *, server_hostname):
            self.assertIs(socket, sock)
            self.assertEqual(server_hostname, host)
            self.assertTrue(context.check_hostname)
            self.assertEqual(context.verify_mode, ssl.CERT_REQUIRED)
            trace.append(('tls', server_hostname))
            clock.sleep(21 if stage == 'tls' else 1)
            return socket
        def parent_tls(connection):
            connection.sock = connection._create_connection(
                (connection.host, connection.port), connection.timeout, connection.source_address)
            connection.sock = connection._context.wrap_socket(connection.sock, server_hostname=connection.host)
        # Keep both stdlib connect tripwires intact. Substitute only the
        # subclass's parent call with memory-only DNS/socket/TLS plumbing;
        # its actual tunnel, deadline, track and rearm checks remain executed.
        connection_source = textwrap.dedent(inspect.getsource(http_transport.DeadlineHTTPSConnection.connect))
        memory_connect = _source_inverse(http_transport.DeadlineHTTPSConnection.connect,
            connection_source, connection_source.replace('super().connect()', '_a64_memory_parent(self)', 1))
        original_builder = http_transport.build_opener
        def built(deadline):
            deadlines.append(deadline)
            return original_builder(deadline)
        with tp.offline_policy() as cap:
            context = self.context(capability=cap)
            accepted, error, provenance = None, None, {}
            with (patch.object(http_transport.time, 'monotonic', clock.now),
                  patch.object(http_transport, '_lookup_addresses', lookup),
                  patch.object(http_transport, '_make_socket', return_value=sock),
                  patch.object(http_transport, '_interrupt_socket',
                      lambda socket: trace.append(('interrupt', socket is sock))),
                  patch.object(http_transport, '_DNS_SLOT', threading.BoundedSemaphore(1)),
                  patch.object(http_transport, '_a64_memory_parent', parent_tls, create=True),
                  patch.object(http_transport.DeadlineHTTPSConnection, 'connect', memory_connect),
                  patch.object(ssl.SSLContext, 'wrap_socket', tls),
                  patch.object(http_transport, 'build_opener', built)):
                try:
                    accepted = membership.guarded_membership_attempt(context.worker('membership-wire'),
                        producer, branch, timeout=20, cancel=cancelled, _provenance=provenance)
                except BaseException as raised:
                    error = raised
            context.seal()
            audit = inspect_ledger(context.ledger.path)
        self.assertTrue(all(not item._timer.is_alive() and item._sockets == [] for item in deadlines))
        return dict(accepted=accepted, error=error, trace=trace, chunks=chunks, wire=wire,
                    audit=audit, provenance=provenance, clock=clock.now())

    def test_a64_membership_actual_https_deadline_and_cleanup_matrix(self):
        import sp500
        for endpoint in ('sp500.github', 'sp500.wikipedia'):
            raw = self.a64_membership_bytes(endpoint == 'sp500.wikipedia')
            positive = self.a64_membership_wire(endpoint, raw)
            self.assertIsNone(positive['error'])
            self.assertEqual(positive['accepted'], sp500.parse_datahub(self.a64_membership_bytes().decode()))
            self.assertEqual(sum(isinstance(row, tuple) and row[0] == 'send' for row in positive['trace']), 1)
            self.assertEqual(positive['audit']['events'][1]['payload']['raw_response_digest'],
                             hashlib.sha256(raw).hexdigest())
            self.assertTrue(positive['wire'].closed)
            for stage in ('dns', 'connect', 'tls', 'headers', 'body'):
                with self.subTest(endpoint=endpoint, stage=stage):
                    run = self.a64_membership_wire(endpoint, raw, stage=stage)
                    self.assertIsInstance(run['error'], TimeoutError)
                    self.assertIsNone(run['accepted'])
                    self.assertEqual(run['provenance'], {})
                    self.assertEqual([row['event'] for row in run['audit']['events']], ['decision', 'result', 'seal'])
                    sends = sum(isinstance(row, tuple) and row[0] == 'send' for row in run['trace'])
                    self.assertEqual(sends, int(stage in ('headers', 'body')))
            for primary in (False, True):
                with self.subTest(endpoint=endpoint, close=primary):
                    run = self.a64_membership_wire(endpoint, raw, close_fault=True,
                        stage='primary-and-close' if primary else None)
                    self.assertIsInstance(run['error'], AuthorityError if primary else LedgerError)
                    self.assertIsNone(run['accepted'])
                    self.assertEqual(run['provenance'], {})
                    self.assertTrue(run['wire'].closed)
            cancellation = threading.Event()
            run = self.a64_membership_wire(endpoint, raw, stage='cancel-body', cancelled=cancellation)
            self.assertIsInstance(run['error'], RequestCancelled)
            self.assertIsNone(run['accepted'])
            self.assertEqual(len(run['chunks']), 1, 'cancel stops before a second chunk')
            self.assertTrue(run['wire'].closed)
            for code in (301, 302, 303, 307, 308, 404, 429, 503):
                with self.subTest(endpoint=endpoint, code=code):
                    run = self.a64_membership_wire(endpoint, b'error body must not be parsed', code=code)
                    self.assertIsInstance(run['error'], HTTPError)
                    self.assertEqual(run['error'].code, code)
                    self.assertIsNone(run['accepted'])
                    self.assertEqual(run['chunks'], [])
                    self.assertTrue(run['wire'].closed)
                    self.assertEqual(sum(isinstance(row, tuple) and row[0] == 'send' for row in run['trace']), 1)

    def test_a64_membership_actual_caps_and_quarantine(self):
        from fetch_ledger import HTTP_BODY_CAPS, HTTP_EVIDENCE_BUDGET
        self.assertEqual(HTTP_EVIDENCE_BUDGET, 128 * 1024 * 1024)
        for endpoint, cap in (('sp500.github', 1024 * 1024), ('sp500.wikipedia', 4 * 1024 * 1024)):
            self.assertEqual(HTTP_BODY_CAPS[endpoint], cap)
            base = self.a64_membership_bytes(endpoint == 'sp500.wikipedia')
            for extra in (0, 1):
                with self.subTest(endpoint=endpoint, extra=extra):
                    raw = base + b'\n' + b' ' * (cap + extra - len(base) - 1)
                    run = self.a64_membership_wire(endpoint, raw)
                    self.assertTrue(all(0 < size <= 65536 for size in run['chunks']))
                    self.assertTrue(run['wire'].closed)
                    self.assertEqual(run['audit']['events'][1]['payload']['raw_response_digest'],
                                     hashlib.sha256(raw).hexdigest())
                    if extra:
                        self.assertIsInstance(run['error'], ResponseQuarantined)
                        self.assertIsNone(run['accepted'])
                        self.assertEqual(run['provenance'], {})
                        self.assertEqual(sum(run['chunks']), cap + 1)
                        self.assertTrue(run['error'].quarantine_path)
                    else:
                        self.assertIsNone(run['error'])
                        self.assertEqual(len(run['accepted']), 505)
                        self.assertEqual(run['provenance']['observed_bytes'], cap)

    def a64_membership_governor_witness(self, *, status=429, retry_after='5',
                                        outcome='master', budget=360, default=False):
        """Actual root fallbacks; each next send inspects its durable predecessor."""
        import sp500
        self.clock, times, bodies = Clock(), [], []
        self.governors = GovernorRegistry(time_fn=self.clock.now, sleep_fn=self.clock.sleep)
        def sending(url, scratch):
            audits = [inspect_ledger(path, require_seal=False)
                      for path in (scratch / 'ledgers').glob('*.jsonl')]
            self.assertEqual(len(audits), 1)
            events = audits[0]['events']
            self.assertEqual(sum(row['event'] == 'decision' for row in events), len(times) + 1)
            self.assertEqual(sum(row['event'] == 'result' for row in events), len(times),
                             'membership fallback requires prior durable result before next wire')
            times.append((url, self.clock.now()))
            if '/main/' in url:
                body = io.BytesIO(b'unread error fixture')
                bodies.append(body)
                raise HTTPError(url, status, 'fixture', {'Retry-After': retry_after}, body)
        deadlines = []
        original_builder = http_transport.build_opener
        def built(deadline):
            deadlines.append((deadline.target.endpoint_id, deadline.end - self.clock.now()))
            return original_builder(deadline)
        with (patch.object(sp500.time, 'monotonic', self.clock.now),
              patch.object(http_transport, 'build_opener', built)):
            run = self.a64_membership_exchange(outcome=outcome, default=default,
                old=b'last-good-governor-cache', before_send=sending,
                options={'_budget_seconds': budget})
        for body in bodies:
            body.close()  # Injected error bodies belong to the local fixture.
        return run, times, deadlines

    def test_a64_membership_governor_fallback_and_context_survival(self):
        import sp500
        from email.utils import format_datetime
        from datetime import timezone
        for status in (429, 503):
            for dated in (False, True):
                retry = (format_datetime(datetime.now(timezone.utc) + timedelta(seconds=9), usegmt=True)
                         if dated else '5')
                with self.subTest(status=status, dated=dated):
                    run, times, _ = self.a64_membership_governor_witness(status=status, retry_after=retry)
                    self.assertIsNone(run['error'])
                    self.assertEqual(run['result'][1], 'datahub')
                    self.assertEqual(len(times), 2)
                    self.assertIn('/main/', times[0][0])
                    self.assertIn('/master/', times[1][0])
                    self.assertGreaterEqual(times[1][1] - times[0][1], 7 if dated else 5)
                    events = run['audits'][0]['events']
                    decisions = [row for row in events if row['event'] == 'decision']
                    self.assertEqual([row['payload']['governor_identity'] for row in decisions],
                                     [['http', 'githubusercontent.com', 'public']] * 2)
                    failure = next(row for row in events if row['event'] == 'result')
                    self.assertEqual(failure['payload']['http_error']['status'], status)
                    self.assertEqual(failure['payload']['http_error']['retry_after'], retry)
                    github = self.governors.provider('sp500.github')
                    self.assertTrue(github._pacer._forced)
                    self.assertEqual(github._pacer.max_requests, 60)
                    self.assertGreaterEqual(github.policy.min_gap_s, 1)
                    stamps = []
                    with patch.object(sp500.time, 'monotonic', self.clock.now):
                        later = self.a64_membership_exchange('fetch',
                            before_send=lambda url, scratch: stamps.append(self.clock.now()))
                    self.assertIsNone(later['error'])
                    self.assertEqual(later['clocks'], ['wall'])
                    self.assertNotEqual(later['audits'][0]['operation_id'], run['audits'][0]['operation_id'])
                    self.assertIs(self.governors.provider('sp500.github'), github)
                    self.assertGreaterEqual(stamps[0] - times[-1][1], 1)
        run, times, _ = self.a64_membership_governor_witness(outcome='wiki')
        self.assertIsNone(run['error'])
        self.assertEqual(run['result'][1], 'wikipedia')
        self.assertEqual(len(times), 3)
        self.assertGreaterEqual(times[1][1] - times[0][1], 5)
        self.assertEqual(times[2][1], times[1][1], 'first Wikipedia turn is independent of GitHub saturation')
        self.assertEqual(run['audits'][0]['events'][-3]['payload']['governor_identity'],
                         ['http', 'wikipedia.org', 'public'])

    def test_a64_membership_governor_budget_and_remaining_wire_deadline(self):
        import fetch_http_membership as membership
        import sp500
        run, times, _ = self.a64_membership_governor_witness(retry_after='120')
        self.assertIsInstance(run['error'], AuthorityError)
        self.assertEqual(len(times), 1)
        self.assertEqual(run['current'], run['old'])
        self.assertEqual([row['event'] for row in run['audits'][0]['events']], ['decision', 'result'])
        github = self.governors.provider('sp500.github')
        self.assertGreaterEqual(github._pacer._not_before, 120)
        # The excessive GitHub wait survives the failed operation but does not
        # delay Wikipedia. This is a new real logical request, not a borrowed root.
        with tp.offline_policy() as cap:
            context = self.context(capability=cap)
            sent = []
            value = membership.guarded_membership_attempt(context.worker('isolated-wikipedia'),
                'http.sp500.wikipedia', 'wikipedia',
                lambda url: sent.append(self.clock.now()) or self.a64_membership_bytes(True))
            context.seal()
        self.assertEqual(len(value), 505)
        self.assertEqual(sent, [0])
        self.assertEqual(inspect_ledger(context.ledger.path)['events'][0]['payload']['governor_identity'],
                         ['http', 'wikipedia.org', 'public'])
        run, times, _ = self.a64_membership_governor_witness(retry_after='5', budget=2)
        self.assertIsInstance(run['error'], RequestRefused)
        self.assertEqual(len(times), 1, 'root budget ends the sequence before master or Wikipedia')
        self.assertEqual(run['current'], run['old'])
        self.assertFalse(run['audits'][0]['verified'])
        self.assertGreaterEqual(self.governors.provider('sp500.github')._pacer._not_before, 5)
        run, times, deadlines = self.a64_membership_governor_witness(retry_after='3', budget=5, default=True)
        self.assertIsNone(run['error'])
        self.assertEqual(len(times), 2)
        self.assertEqual(len(deadlines), 2)
        self.assertEqual(deadlines[0], ('sp500.github', 5))
        self.assertGreater(deadlines[1][1], 0)
        self.assertLessEqual(deadlines[1][1], 2, 'pacing cannot reset the remaining root wire budget')

    def test_a64_membership_governor_removal_inverses(self):
        import fetch_governors as governors
        import fetch_http_membership as membership
        # Correct input is accepted by the real root. Each mutant is then run
        # against that same oracle; no direct governor-only result earns credit.
        run, times, _ = self.a64_membership_governor_witness()
        self.assertIsNone(run['error'])
        self.assertGreaterEqual(times[1][1], 5)
        function = governors.Governor.http_backoff
        source = textwrap.dedent(inspect.getsource(function))
        weakened = _source_inverse(function, 'delay = max(local, server)', 'delay = local')
        with patch.object(governors.Governor, 'http_backoff', weakened), self.assertRaises(AssertionError):
            run, times, _ = self.a64_membership_governor_witness()
            self.assertIsNone(run['error'])
            self.assertGreaterEqual(times[1][1], 5, 'membership server wait cannot be shortened')
        print('A64_MEMBERSHIP_GOVERNOR_INVERSE_RED shortened-Retry-After')
        function = membership.guarded_membership_attempt
        source = textwrap.dedent(inspect.getsource(function))
        separated = _source_inverse(function, 'context.governors.provider(envelope.endpoint),',
                                    'context.governors.provider(envelope.endpoint, credential=branch),')
        with patch.object(membership, 'guarded_membership_attempt', separated), self.assertRaises(AssertionError):
            run, _, _ = self.a64_membership_governor_witness()
            self.assertIsNone(run['error'], 'membership root requires its canonical shared governor')
        print('A64_MEMBERSHIP_GOVERNOR_INVERSE_RED branch-split-identity')
        function = membership.guarded_membership_attempt
        restarted = _source_inverse(function, 'wire_timeout = min(timeout, remaining_root)',
                                    'wire_timeout = timeout')
        with patch.object(membership, 'guarded_membership_attempt', restarted), self.assertRaises(AssertionError):
            run, _, deadlines = self.a64_membership_governor_witness(retry_after='3', budget=5, default=True)
            self.assertIsNone(run['error'])
            self.assertLessEqual(deadlines[1][1], 2, 'remaining operation deadline cannot restart after pacing')
        print('A64_MEMBERSHIP_GOVERNOR_INVERSE_RED restarted-wire-budget')
        original = FetchLedger.result
        def hidden(ledger, ids, payload):
            if payload.get('error_type') == 'HTTPError':
                return None
            return original(ledger, ids, payload)
        with patch.object(FetchLedger, 'result', hidden), self.assertRaises(AssertionError):
            run, _, _ = self.a64_membership_governor_witness()
            self.assertIsNone(run['error'], 'fallback cannot hide its predecessor result')
        print('A64_MEMBERSHIP_GOVERNOR_INVERSE_RED hidden-fallback-result')

    def test_a64_membership_reviewed_floor_refusal_and_removal_inverse(self):
        import fetch_governors as governors
        import fetch_envelopes as envelopes
        from dataclasses import replace
        def witness(endpoint):
            domain = governors.ENDPOINTS[endpoint].domain
            policies = dict(governors.HTTP_POLICIES)
            policies[domain] = replace(policies[domain], min_gap_s=0.5, burst_window_s=0.5)
            self.clock = Clock()
            self.governors = GovernorRegistry(time_fn=self.clock.now, sleep_fn=self.clock.sleep)
            with patch.object(governors, 'HTTP_POLICIES', policies):
                run = self.a64_membership_exchange(outcome='main' if endpoint == 'sp500.github' else 'wiki',
                                                   old=b'last-good reviewed-floor cache')
            self.assertIsInstance(run['error'], AuthorityError, 'actual membership root must reject weaker policy')
            self.assertIn('weaker than reviewed floor', str(run['error']))
            self.assertEqual(len(run['calls']), 0 if endpoint == 'sp500.github' else 2)
            self.assertFalse(any('wikipedia.org' in url for url in run['calls']))
            self.assertIsNone(run['result'])
            self.assertEqual(run['current'], run['old'])
            self.assertFalse(run['reports'][0]['published'])
        source = textwrap.dedent(inspect.getsource(governors.endpoint_policy))
        begin = source.index('    if (policy.min_gap_s < floor')
        end = source.index('\n        raise AuthorityError("HTTP policy weaker', begin)
        inverse = _source_inverse(governors.endpoint_policy, source[begin:end], '    if False:')
        for endpoint in ('sp500.github', 'sp500.wikipedia'):
            with self.subTest(endpoint=endpoint):
                witness(endpoint)
                # The envelope owner imports the same shared function. Rebind
                # both references to this one body mutant, not two guards.
                with (patch.object(governors, 'endpoint_policy', inverse),
                      patch.object(envelopes, 'endpoint_policy', inverse),
                      self.assertRaises(AssertionError)):
                    witness(endpoint)
                print('A64_MEMBERSHIP_GOVERNOR_INVERSE_RED reviewed-floor', endpoint)

    def test_a64_all_eight_http_execution_bindings_and_inverse_reconciliation(self):
        import sys
        import fetch_operations as operations
        import fetch_governors as governors
        import fetch_ledger as ledger
        import split_provider
        import sp500
        inventory = fetch_send_inventory.build_inventory()
        site_rows = {row['site_key']: row for row in inventory['sites']}
        expected_pairs = {
            ('http.stockanalysis.validation', 'stockanalysis.history', 'http-series'),
            ('http.stockanalysis.external_sweep', 'stockanalysis.history', 'http-series'),
            ('http.split_provider.yahoo', 'yahoo.splits', 'http-series'),
            ('http.split_provider.sec_tickers', 'sec.tickers', 'http-metadata'),
            ('http.split_provider.sec_facts', 'sec.companyfacts', 'http-metadata'),
            ('http.split_provider.issuer', 'fortinet.history', 'http-metadata'),
            ('http.sp500.github', 'sp500.github', 'http-metadata'),
            ('http.sp500.wikipedia', 'sp500.wikipedia', 'http-metadata'),
        }
        owners = {
            'http.stockanalysis.validation': ('stock_validate.py', 'fetch_daily_reference'),
            'http.stockanalysis.external_sweep': ('external_sweep.py', '_read'),
            **{producer: ('split_provider.py', '_attempt') for producer in (
                'http.split_provider.yahoo', 'http.split_provider.sec_tickers',
                'http.split_provider.sec_facts', 'http.split_provider.issuer')},
            'http.sp500.github': ('sp500.py', '_http'),
            'http.sp500.wikipedia': ('sp500.py', '_http'),
        }
        by_producer = {row[0]: row for row in _A64_HTTP_BINDINGS}
        physical_selectors = {(row['module'].split('.')[-1] + '.py',
                               row['qualname'].replace('.', '.<locals>.', 1)):
                              row for row in site_rows.values()
                              if row['site_key'] in {binding[3] for binding in _A64_HTTP_BINDINGS}}
        self.assertEqual(len(physical_selectors), 3)
        pending, sent, returned, results, logical_dispatches = {}, [], set(), set(), []
        original_decision, original_result = FetchLedger.decision, FetchLedger.result
        def decision(ledger, ids, payload):
            saved = original_decision(ledger, ids, payload)
            producer = ids['producer_id']
            self.assertIn(producer, by_producer)
            frames, frame = set(), inspect.currentframe().f_back
            while frame is not None:
                frames.add((Path(frame.f_code.co_filename).name, frame.f_code.co_name))
                frame = frame.f_back
            self.assertIn(owners[producer], frames, 'HTTP expansion must traverse its actual owner')
            if producer == 'http.stockanalysis.external_sweep':
                self.assertIn(('external_sweep.py', '_provider_fetch'), frames)
                logical_dispatches.append((ids['operation_id'], ids['attempt_id']))
            self.assertNotIn(threading.get_ident(), pending)
            pending[threading.get_ident()] = (dict(ids), deepcopy(payload))
            return saved
        def result(ledger, ids, payload):
            saved = original_result(ledger, ids, payload)
            results.add((ids['operation_id'], ids['attempt_id']))
            if payload['outcome'] == 'returned':
                returned.add(ids['producer_id'])
            return saved
        def profile(frame, event, arg):
            if event != 'call' or 'wire' not in frame.f_locals or frame.f_back is None:
                return
            parent = frame.f_back
            key = (Path(parent.f_code.co_filename).name, parent.f_code.co_qualname)
            site = physical_selectors.get(key)
            if site is None:
                return
            self.assertEqual(parent.f_lineno, site['line'], 'HTTP physical line must match actual inventory')
            ids, payload = pending.pop(threading.get_ident())
            binding = by_producer[ids['producer_id']]
            requested = payload['requested']
            self.assertEqual((requested['endpoint'], requested['variant']), binding[1:3])
            self.assertEqual(site['site_key'], binding[3])
            self.assertEqual(frame.f_locals['wire'].full_url, payload['http_request']['url'])
            self.assertEqual(payload['governor_identity'],
                             ['http', governors.ENDPOINTS[binding[1]].domain, 'public'])
            sent.append((ids['producer_id'], requested['endpoint'], requested['variant'],
                         site['site_key'], ids['operation_id'], ids['attempt_id']))
        previous_profile = sys.getprofile()
        with patch.object(FetchLedger, 'decision', decision), patch.object(FetchLedger, 'result', result):
            sys.setprofile(profile)
            try:
                self.test_a55_default_success_all_six_direct_roots()
                self.a52_ftnt_complete_witness('single')
                for root, outcome in (('current', 'wiki'), ('fetch', 'main')):
                    run = self.a64_membership_exchange(root, default=True, outcome=outcome)
                    self.assertIsNone(run['error'])
                    self.assertTrue(run['audits'][0]['verified'])
            finally:
                sys.setprofile(previous_profile)
        self.assertEqual(pending, {})
        self.assertEqual({(row[4], row[5]) for row in sent}, results)
        self.assertEqual(len(sent), len(results), 'each physical open has exactly one durable result')
        self.assertEqual(returned, {row[0] for row in expected_pairs}, 'all eight have accepted runtime evidence')
        self.assertEqual(len(logical_dispatches), 2)
        self.assertTrue(set(logical_dispatches).issubset(results))
        logical_site = site_rows['engine.external_sweep:_provider_fetch:provider_fetch:1']
        self.assertEqual(logical_site['disposition'], 'a2_refused')
        self.assertNotIn('http.stockanalysis.provider_dispatch', {row[0] for row in sent})
        # The five historical holds have two explicit retired compatibility
        # entries, not two forgotten sends. Four lineage owners map to three
        # actual owned opens. Default roots refuse before any effect.
        for entry in (lambda: split_provider._default_fetcher('https://example.invalid'),
                      lambda: split_provider.YahooSecProvider()._fetch('https://example.invalid')):
            with self.assertRaises(AuthorityError):
                entry()
        holds = []
        for purpose, entry in (
            ('validation', lambda: stock_validate.fetch_daily_reference_single_request('TEST', 'Max')),
            ('external_sweep', lambda: external_sweep.sweep_ticker(Path(self.temp.name) / 'held-bank', 'TEST',
                provider=external_sweep.StockAnalysisProvider())),
            ('membership', lambda: sp500.current_sp500(cache_path=Path(self.temp.name) / 'held-cache')),
        ):
            with patch.object(FetchRunContext, 'create', side_effect=AssertionError('held root created context')):
                with self.assertRaisesRegex(AuthorityError, 'not activated'):
                    entry()
            holds.append(purpose)
        for root in ('single', 'batch', 'cli'):
            run = self.a52_root_exchange(root, capability=False, old_good=True)
            self.assertIsInstance(run['error'], AuthorityError)
            self.assertEqual(run['calls'], [])
            self.assertEqual(run['clocks'], [])
            self.assertEqual(run['audits'], [])
            self.assertEqual(run['old'], run['current'])
        holds.append('split_refresh')
        self.assertEqual(set(holds), {'validation', 'external_sweep', 'split_refresh', 'membership'})
        for module, name in ((http_transport, '_TARGET_HOSTS'), (ledger, 'HTTP_BODY_CAPS')):
            tree = ast.parse(Path(module.__file__).read_text(encoding='utf-8'))
            value = next(node.value.args[0] for node in tree.body if isinstance(node, ast.Assign)
                         and any(isinstance(target, ast.Name) and target.id == name for target in node.targets))
            keys = [ast.literal_eval(key) for key in value.keys]
            self.assertEqual(len(keys), len(set(keys)), name + ' cannot hide duplicate literal endpoints')
            self.assertEqual(set(keys), {row[1] for row in expected_pairs})
        def verify(bindings, sites=site_rows, rights=operations.PURPOSE_PRODUCERS, actual=sent):
            assert len(bindings) == 8 and len({row[0] for row in bindings}) == 8, 'duplicate or missing expansion'
            assert {(row[0], row[1], row[2]) for row in bindings} == expected_pairs, 'wrong endpoint/variant expansion'
            assert {(producer, endpoint, variant) for producer, endpoint, variant, *_ in actual} == expected_pairs
            assert {(row[0], row[1], row[2], row[3]) for row in bindings} == {row[:4] for row in actual}
            assert {row[4] for row in bindings} == {
                'stock_validate.fetch_daily_reference', 'external_sweep.StockAnalysisProvider._read',
                'split_provider._default_fetcher', 'sp500._http'}, 'lost historical seam lineage'
            assert all(sites[row[3]]['disposition'] == 'a2_refused' for row in bindings), 'permissive physical site'
            assert all(row[0] in rights[row[5]] for row in bindings), 'expansion lost its held purpose right'
            assert not any(value.startswith('http.') for value in rights['nightly']), 'HTTP in nightly'
            assert not any(value.startswith('http.') for value in rights['add_stocks']), 'HTTP in add_stocks'
            assert not any(value.startswith('http.') for value in operations._SHARED), 'HTTP in shared rights'
            assert len(results) == len(actual), 'logical-as-physical duplicate pair'
        verify(_A64_HTTP_BINDINGS)
        first = _A64_HTTP_BINDINGS[0]
        for label, changed in (
            ('missing', _A64_HTTP_BINDINGS[:-1]),
            ('duplicate-plus-gap', _A64_HTTP_BINDINGS[:-1] + (_A64_HTTP_BINDINGS[0],)),
            ('extra', _A64_HTTP_BINDINGS + (first,)),
            ('wrong-endpoint', ((first[0], 'sp500.github') + first[2:],) + _A64_HTTP_BINDINGS[1:]),
            ('lost-membership-lineage', _A64_HTTP_BINDINGS[:-1] + (
                _A64_HTTP_BINDINGS[-1][:4] + ('unknown-owner',) + _A64_HTTP_BINDINGS[-1][5:],)),
        ):
            with self.subTest(mutant=label), self.assertRaises(AssertionError):
                verify(changed)
            print('A64_ALL_HTTP_BINDING_INVERSE_RED', label)
        changed_sites = deepcopy(site_rows)
        changed_sites[first[3]]['disposition'] = 'allowed'
        with self.assertRaisesRegex(AssertionError, 'permissive physical site'):
            verify(_A64_HTTP_BINDINGS, sites=changed_sites)
        print('A64_ALL_HTTP_BINDING_INVERSE_RED permissive-site')
        for purpose in ('nightly', 'add_stocks'):
            rights = dict(operations.PURPOSE_PRODUCERS)
            rights[purpose] = rights[purpose] | {'http.sp500.github'}
            with self.assertRaisesRegex(AssertionError, 'HTTP in ' + purpose):
                verify(_A64_HTTP_BINDINGS, rights=rights)
            print('A64_ALL_HTTP_BINDING_INVERSE_RED HTTP-in-' + purpose)
        with self.assertRaisesRegex(AssertionError, 'logical-as-physical'):
            verify(_A64_HTTP_BINDINGS, actual=sent + [sent[0]])
        print('A64_ALL_HTTP_BINDING_INVERSE_RED logical-as-physical')
        print('A64_ALL_HTTP_EXECUTION_REACHED eight expansions/seven endpoints/four lineages/five holds;'
              ' three exact owned opens, one logical dispatcher, control probe excluded;', len(sent), 'physical pairs')

    def test_a64_membership_finite_evidence_preflight_and_wire_tamper(self):
        import fetch_http_membership as membership
        for producer, branch in (('http.sp500.github', 'main'), ('http.sp500.github', 'master'),
                                 ('http.sp500.wikipedia', 'wikipedia')):
            envelope, evidence = membership._spec(producer, branch)
            class ImpersonatedEvidence(membership._MembershipEvidence):
                def __eq__(self, other):
                    return True
            invalid = [None, SimpleNamespace(**vars(evidence)), ImpersonatedEvidence(**vars(evidence)),
                replace(evidence, url=evidence.url + '?query=1'),
                replace(evidence, url=evidence.url + '#fragment'),
                replace(evidence, url=evidence.url.replace('//', '//user@', 1)),
                replace(evidence, url=evidence.url.replace('https://', 'https://other.invalid/', 1)),
                replace(evidence, url=evidence.url.replace('.org/', '.org:444/').replace('.com/', '.com:444/')),
                replace(evidence, url=evidence.url.replace('/data/constituents.csv', '/data/other.csv')
                    if branch != 'wikipedia' else evidence.url.replace('/wiki/', '/w/')),
                replace(evidence, url=evidence.url.replace('https://', 'http://')),
                replace(evidence, url='https://user:password@example.com:444/path#fragment'),
                replace(evidence, headers=(('Accept', 'anything'),)),
                replace(evidence, producer='http.sp500.unknown'),
                replace(evidence, branch='unreviewed-branch')]
            for value in invalid:
                with self.subTest(producer=producer, value=type(value).__name__), tp.offline_policy() as cap:
                    context = self.context(capability=cap)
                    with self.assertRaises(AuthorityError):
                        request = context.worker('bad-membership').request(producer, envelope.wire(), _http_binding=value)
                        request.execute(lambda effective: self.fail('invalid evidence reached wire'),
                            acquire_turn=lambda: 0, governor=context.governors.provider(envelope.endpoint),
                            normalizer=lambda raw: raw, attempt_evidence=value)
                    self.assertEqual(context.ledger.path.read_bytes(), b'')
            for changed in (replace(envelope, subject='OTHER'),
                            replace(envelope, endpoint='sp500.wikipedia' if branch != 'wikipedia' else 'sp500.github')):
                with self.subTest(envelope=changed.wire()), tp.offline_policy() as cap:
                    context = self.context(capability=cap)
                    with self.assertRaises(AuthorityError):
                        context.worker('bad-envelope').request(producer, changed.wire(), _http_binding=evidence)
                    self.assertEqual(context.ledger.path.read_bytes(), b'')
            for key, value in (('url', 'https://other.invalid/'), ('headers', (('Accept', 'anything'),))):
                captured, sent = [], []
                original_spec = membership._spec
                def capture(*args):
                    pair = original_spec(*args)
                    captured.append(pair[1])
                    return pair
                def fault(event, stage):
                    if event == 'decision' and stage == 'fsync':
                        object.__setattr__(captured[0], key, value)
                with self.subTest(wire_tamper=key), tp.offline_policy() as cap:
                    context = FetchRunContext.create(Path(self.temp.name) / 'wire-tamper',
                        authority=self.authority, clock=lambda: datetime(2026, 8, 4, 17, tzinfo=NY),
                        governors=self.governors, test_capability=cap, fault=fault)
                    self.addCleanup(context.ledger.close)
                    with patch.object(membership, '_spec', capture), self.assertRaisesRegex(
                            RequestRefused, 'provenance changed'):
                        membership.guarded_membership_attempt(context.worker('wire-tamper'), producer, branch,
                            lambda url: sent.append(url) or self.a64_membership_bytes(branch == 'wikipedia'))
                    context.seal()
                    audit = inspect_ledger(context.ledger.path)
                self.assertEqual(sent, [])
                self.assertEqual([row['event'] for row in audit['events']], ['decision', 'result', 'seal'])
                self.assertEqual(audit['events'][1]['payload']['error_type'], 'RequestRefused')

    def test_a64_membership_missing_expired_policy_and_cap_drift(self):
        import fetch_http_membership as membership
        import sp500
        with tp.offline_policy() as expired:
            pass
        for root in (sp500.current_sp500, sp500.fetch_current_sp500):
            directory = Path(self.temp.name) / root.__name__
            with self.subTest(root=root.__name__), self.assertRaises(AuthorityError):
                root(_test_capability=expired, _evidence_dir=directory,
                     _clock=lambda: self.fail('expired membership policy reached clock'),
                     _opener=lambda url: self.fail('expired membership policy reached send'))
            self.assertFalse(directory.exists())
        for producer, branch in (('http.sp500.github', 'main'), ('http.sp500.wikipedia', 'wikipedia')):
            envelope, evidence = membership._spec(producer, branch)
            for at_wire in (False, True):
                caps = dict(membership.HTTP_BODY_CAPS)
                if not at_wire:
                    caps[envelope.endpoint] += 1
                sent = []
                def fault(event, stage):
                    if at_wire and event == 'decision' and stage == 'fsync':
                        caps[envelope.endpoint] += 1
                with self.subTest(producer=producer, at_wire=at_wire), tp.offline_policy() as cap:
                    context = FetchRunContext.create(Path(self.temp.name) / 'cap-drift',
                        authority=self.authority, clock=lambda: datetime(2026, 8, 4, 17, tzinfo=NY),
                        governors=self.governors, test_capability=cap, fault=fault)
                    self.addCleanup(context.ledger.close)
                    with patch.object(membership, 'HTTP_BODY_CAPS', caps), self.assertRaisesRegex(
                            RequestRefused, 'reviewed body cap changed'):
                        membership.guarded_membership_attempt(context.worker('cap-drift'), producer, branch,
                            lambda url: sent.append(url) or self.a64_membership_bytes(branch == 'wikipedia'))
                    if at_wire:
                        context.seal()
                        self.assertEqual([row['event'] for row in inspect_ledger(context.ledger.path)['events']],
                                         ['decision', 'result', 'seal'])
                    else:
                        self.assertEqual(context.ledger.path.read_bytes(), b'')
                self.assertEqual(sent, [])

    def test_a64_membership_binding_terminal_and_cache_removal_inverses(self):
        import fetch_http_membership as membership
        import fetch_operations as operations
        import sp500
        binding = membership.verified_membership_attempt_evidence
        source = textwrap.dedent(inspect.getsource(binding))
        cases = (
            (binding, 'if type(evidence) is not _MembershipEvidence or type(producer) is not str:',
             'if not isinstance(evidence, _MembershipEvidence) or type(producer) is not str:',
             'test_a64_membership_finite_evidence_preflight_and_wire_tamper', 'exact-typed-evidence'),
            (binding, 'if envelope != expected_envelope or evidence != expected:', 'if False:',
             'test_a64_membership_finite_evidence_preflight_and_wire_tamper', 'finite-envelope-URL-headers'),
            (binding, 'if HTTP_BODY_CAPS.get(envelope.endpoint) != _CAPS[envelope.endpoint]:', 'if False:',
             'test_a64_membership_missing_expired_policy_and_cap_drift', 'reviewed-cap-drift'),
            (membership.guarded_membership_attempt,
             'verified_membership_attempt_evidence(context, producer, effective, evidence)', 'pass',
             'test_a64_membership_finite_evidence_preflight_and_wire_tamper', 'immediate-wire-rederivation'),
            (sp500._provider_failure,
             'if isinstance(error, (AuthorityError, LedgerError, RequestCancelled, ResponseQuarantined)):\n        raise error',
             'if isinstance(error, (AuthorityError, LedgerError, RequestCancelled, ResponseQuarantined)):\n        return True',
             'test_a64_membership_terminals_never_fallback_or_replace_cache', 'terminal-swallow'),
            (sp500._cache_valid, "provenance['accepted_digest'] != digest_value(tickers)", 'False',
             'test_a64_membership_cache_provenance_tampering_has_no_freshness', 'accepted-membership-digest'),
            (sp500._run_membership, 'operation.close()  # Cleanup is terminal and precedes any cache change.',
             "\n            if candidate is not None and cache is not None:\n                _save(cache, candidate)\n            operation.close()  # Cleanup is terminal and precedes any cache change.",
             'test_a64_membership_cleanup_precedes_cache_publication', 'early-cache-publication'),
        )
        for function, old, new, witness, label in cases:
            owner = membership if function.__module__ == membership.__name__ else sp500
            mutant = _source_inverse(function, old, new)
            prior_operations, prior_children = set(operations._OPERATIONS), set(operations._CHILDREN)
            try:
                with (self.subTest(label=label), patch.object(owner, function.__name__, mutant),
                      patch.object(self, 'subTest', lambda *args, **kwargs: nullcontext())):
                    with self.assertRaises(AssertionError):
                        getattr(self, witness)()
            finally:
                # The intentionally early-write mutant can bypass the real
                # close call. Restore real source first, then drain only this
                # local fixture's joined handles; never reset shared registries.
                self.assertEqual(set(operations._CHILDREN), prior_children)
                for handle in set(operations._OPERATIONS) - prior_operations:
                    handle.close()
                self.assertEqual(set(operations._OPERATIONS), prior_operations)
            print('A64_MEMBERSHIP_BINDING_CACHE_INVERSE_RED', label)


    def a65_exact_field_witness(self, kind, mutation, *, only_route=None, receipts=None):
        import fetch_http_membership as membership
        class DishonestString(str):
            def __eq__(self, other):
                return True
        routes = (('http.sp500.github', 'main'), ('http.sp500.github', 'master'),
                  ('http.sp500.wikipedia', 'wikipedia')) if kind == 'membership' else tuple(
            (producer, 'FTNT' if endpoint == 'fortinet.history' else 'TEST')
            for producer, endpoint, _variant, _owner in self._A23B_BINDINGS)
        if only_route is not None:
            self.assertIn(only_route, routes)
            routes = (only_route,)
        for producer, choice in routes:
            with self.subTest(kind=kind, producer=producer, choice=choice), tp.offline_policy() as cap:
                context = self.context(capability=cap)
                if kind == 'membership':
                    envelope, evidence = membership._spec(producer, choice)
                    raw = self.a64_membership_bytes(choice == 'wikipedia')
                    normalizer = lambda body: membership.normalize_membership(body, envelope)
                else:
                    options = self.a59_split_options(producer)
                    envelope, evidence = splits._spec(context, producer, choice,
                        options.get('cik'), options.get('sec_user_agent'))
                    raw = self.a59_split_fixture(producer)
                    normalizer = splits.decode_split_json
                if mutation is None:
                    changed = [evidence]
                elif mutation == 'url':
                    changed = [replace(evidence, url=DishonestString(evidence.url + '&unreviewed=1'))]
                elif mutation == 'user-agent':
                    changed = [replace(evidence, headers=(
                        (evidence.headers[0][0], DishonestString('unreviewed-agent')),
                        *evidence.headers[1:]))]
                else:
                    self.assertEqual(mutation, 'shape')
                    changed = [replace(evidence, producer=DishonestString('unregistered-producer')),
                        replace(evidence, url=None),
                        replace(evidence, headers=list(evidence.headers)),
                        replace(evidence, headers=(list(evidence.headers[0]), *evidence.headers[1:])),
                        replace(evidence, headers=((*evidence.headers[0], 'extra'), *evidence.headers[1:])),
                        replace(evidence, headers=((DishonestString('X-Changed'), evidence.headers[0][1]),
                                                  *evidence.headers[1:])),
                        replace(evidence, headers=((evidence.headers[0][0], 123), *evidence.headers[1:])),
                        replace(evidence, headers=evidence.headers[:2])]
                    changed.append(replace(evidence, **{
                        'branch' if kind == 'membership' else 'symbol': DishonestString('changed')}))
                    if kind == 'split':
                        changed.extend((replace(evidence, cik=True),
                                        replace(evidence, sec_agent=DishonestString('operator@example.invalid'))))
                for value in changed:
                    # A refused logical request is terminal. Each malformed
                    # field starts from a fresh admitted fixture, not a request
                    # already poisoned by an earlier case.
                    local = context if value is evidence else self.context(capability=cap)
                    sender = Mock(return_value=raw)
                    previous_time = self.clock.value
                    with patch.object(local.ledger, 'decision', wraps=local.ledger.decision) as decision:
                        def execute():
                            request = local.worker('a65-exact-field').request(
                                producer, envelope.wire(), _http_binding=value)
                            return request.execute(lambda effective: sender(effective),
                                acquire_turn=lambda: 0, governor=local.governors.provider(envelope.endpoint),
                                normalizer=normalizer, attempt_evidence=value)
                        if mutation is None:
                            self.assertIsNotNone(execute(), 'correct evidence reaches accepted result')
                            self.assertEqual(sender.call_count, 1, 'correct evidence actually sends once')
                            self.assertEqual(decision.call_count, 1)
                            local.seal()
                            self.assertTrue(inspect_ledger(local.ledger.path)['verified'])
                        else:
                            try:
                                with self.assertRaisesRegex(RequestRefused, 'provenance'):
                                    execute()
                                sender.assert_not_called()
                                decision.assert_not_called()
                                self.assertEqual(self.clock.value, previous_time, 'exact fields refuse before turn')
                                self.assertEqual(local.ledger.path.read_bytes(), b'')
                            finally:
                                if receipts is not None:
                                    receipts.append(dict(sender_calls=sender.call_count,
                                        decisions=decision.call_count, events=len(inspect_ledger(
                                            local.ledger.path, require_seal=False)['events'])))

    def a65_exact_field_controls(self, kind):
        import fetch_http_membership as membership
        owner = membership if kind == 'membership' else splits
        function = (membership.verified_membership_attempt_evidence if kind == 'membership'
                    else splits.verified_split_attempt_evidence)
        self.a65_exact_field_witness(kind, None)
        for mutation in ('url', 'user-agent', 'shape'):
            self.a65_exact_field_witness(kind, mutation)
        for mutation, guard in (('url', 'type(evidence.url) is not str'),
                                ('user-agent', 'any(type(value) is not str for value in pair)')):
            inverse = _source_inverse(function, guard, 'False')
            routes = (('http.sp500.github', 'main'), ('http.sp500.github', 'master'),
                      ('http.sp500.wikipedia', 'wikipedia')) if kind == 'membership' else tuple(
                (producer, 'FTNT' if endpoint == 'fortinet.history' else 'TEST')
                for producer, endpoint, _variant, _owner in self._A23B_BINDINGS)
            for producer, choice in routes:
                observed = []
                with (patch.object(owner, function.__name__, inverse),
                      patch.object(self, 'subTest', lambda *args, **kwargs: nullcontext())):
                    with self.assertRaisesRegex(AssertionError, 'RequestRefused not raised'):
                        self.a65_exact_field_witness(kind, mutation,
                            only_route=(producer, choice), receipts=observed)
                self.assertEqual(observed, [dict(sender_calls=1, decisions=1, events=2)],
                                 'guard removal reaches intended sender and durable pair')
                print('A65_EXACT_FIELD_INVERSE_RED', kind, producer, choice, mutation)

    def test_a65_membership_exact_field_guards_and_inverses(self):
        self.a65_exact_field_controls('membership')

    def test_a65_split_exact_field_guards_and_inverses(self):
        self.a65_exact_field_controls('split')

    def a65_membership_binding_witness(self):
        import fetch_http_membership as membership
        with tp.offline_policy() as cap:
            positive = self.context(capability=cap)
            self.assertEqual(len(membership.guarded_membership_attempt(
                positive.worker('a65-binding-positive'), 'http.sp500.github', 'main',
                lambda url: self.a64_membership_bytes())), 505)
            positive.seal()
            self.assertTrue(inspect_ledger(positive.ledger.path)['verified'])
            context = self.context(capability=cap)
            envelope, bound = membership._spec('http.sp500.github', 'main')
            changed_envelope, changed = membership._spec('http.sp500.github', 'master')
            self.assertEqual(changed_envelope, envelope)
            request = context.worker('a65-logical-binding').request(
                'http.sp500.github', envelope.wire(), _http_binding=bound)
            sender = Mock(return_value=self.a64_membership_bytes())
            with self.assertRaisesRegex(RequestRefused, 'membership logical request provenance binding changed'):
                request.execute(lambda effective: sender(), acquire_turn=lambda: 0,
                    governor=context.governors.provider(envelope.endpoint),
                    normalizer=lambda raw: membership.normalize_membership(raw, envelope),
                    attempt_evidence=changed)
            sender.assert_not_called()
            self.assertEqual(context.ledger.path.read_bytes(), b'')

    def test_a65_membership_request_binding_and_inverse(self):
        self.a65_membership_binding_witness()
        inverse = _source_inverse(splits.LogicalRequest._execute,
            'if self._http_binding is None or attempt_evidence != self._http_binding:\n'
            '            raise RequestRefused("membership logical request provenance binding changed")',
            'if False:\n            raise RequestRefused("membership logical request provenance binding changed")')
        with patch.object(splits.LogicalRequest, '_execute', inverse):
            with self.assertRaisesRegex(AssertionError, 'RequestRefused not raised'):
                self.a65_membership_binding_witness()
        print('A65_MEMBERSHIP_LOGICAL_BINDING_INVERSE_RED')


if __name__ == "__main__":
    result = unittest.TextTestRunner(verbosity=2).run(
        unittest.defaultTestLoader.loadTestsFromTestCase(HttpLabels))
    print(f"{result.testsRun} checks, {len(result.errors) + len(result.failures)} failed")
    raise SystemExit(not result.wasSuccessful())
