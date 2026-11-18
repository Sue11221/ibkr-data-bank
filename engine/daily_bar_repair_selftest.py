"""Synthetic-bank, port-free date repair development and regression tests."""
from contextlib import nullcontext
from copy import deepcopy
from datetime import date, datetime
import io
import json
import platform
import os
import subprocess
import sys
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import fetch_test_policy as policy

with policy.offline_transports():
    import numpy  # Load native dependency before patch.dict(sys.modules).
    with patch.object(platform, "machine", return_value="AMD64"):
        import pandas
        import pyarrow
    import daily_bar_repair as repair
    import fetch_governors as governors
    import stock_ibkr as ib
    import stock_storage as storage
    from fetch_authority import AuthorityError, NY, ScheduleAuthority
    from fetch_run_context import FetchRunContext, RequestRefused
    from fetch_ledger import inspect_ledger
    from fetch_ibkr_integration_selftest import RawIB, bar, ib_contract, InstantPacer, OfflineLease


DAY = date(2026, 7, 8)
PAIRS = [{"ticker": "TEST", "date": DAY.isoformat()}]
CRASH_PHASES = ("preparation_unpublished", "prepared", "manifest_published",
                "month_published", "cleanup_payloads_removed", "cleanup_stage_detached")


class InjectedCrash(RuntimeError):
    pass


class DailyRepair(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.authority = ScheduleAuthority.load()

    def setUp(self):
        self.guard = policy.offline_transports()
        self.guard.__enter__()
        self.addCleanup(self.guard.__exit__, None, None, None)
        scratch_parent = getattr(self, "crash_parent", None) or Path(__file__).resolve().parents[1] / "Run Logs"
        scratch_parent.mkdir(exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(prefix="daily-repair-synthetic-", dir=scratch_parent)
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / "synthetic bank"
        self.root.mkdir()
        self.now = datetime(2026, 8, 4, 17, tzinfo=NY)
        self.virtual_time = 0.0
        def sleep(seconds, cancel=None):
            self.virtual_time += seconds
        self.enterContext(patch.object(governors, "_REGISTRY",
            governors.GovernorRegistry(time_fn=lambda: self.virtual_time, sleep_fn=sleep)))
        self.enterContext(patch.object(ib, "_interruptible_sleep", return_value=None))
        self.enterContext(patch.object(repair.operation_gate, "acquire",
                                       side_effect=lambda *a, **k: OfflineLease()))
        self.enterContext(patch.object(ib, "_connection_diag_path",
                                       return_value=Path(self.temp.name) / "diagnostic.log"))
        self.enterContext(patch.dict("sys.modules", {"ib_async": SimpleNamespace(Contract=ib_contract)}))
        create = FetchRunContext.create
        self.contexts = []
        def fixed_create(directory, **kwargs):
            context = create(directory, authority=self.authority,
                             clock=lambda: self.now, **kwargs)
            self.contexts.append(context)
            return context
        self.enterContext(patch.object(FetchRunContext, "create", side_effect=fixed_create))
        self.adapter = ib.LiveIB()
        self.raw = self.adapter.ib = RawIB([bar(DAY)])
        self.adapter._await = lambda response, timeout, what: response
        self.adapter.disconnect = lambda: None
        self.factory_calls = 0
        self.make_month("TEST", "1d", [7, 8, 9], close=9.5)
        self.make_month("OTHER", "1d", [8], close=9.5)
        self.make_month("TEST", "1m", [8], close=9.5)

    def make_month(self, ticker, interval, days, close=10.0, fmt="csv"):
        rows = [(datetime(2026, 7, d, 0 if interval == "1d" else 10),
                 10.0, 11.0, 9.0, close, 100) for d in days]
        path = storage.month_file_path(self.root, ticker, 2026, 7, interval, fmt=fmt)
        path.parent.mkdir(parents=True, exist_ok=True)
        stats = storage.write_month_file(path, rows, verify_after_write=True)
        tdir = self.root / ticker
        manifest = storage.load_manifest(tdir) or storage.new_manifest(ticker, ticker)
        manifest["conid"] = 123
        storage.manifest_months(manifest, interval)["2026-07"] = dict(stats, status="present")
        storage.save_manifest(tdir, manifest)
        return path

    def factory(self):
        self.factory_calls += 1
        return self.adapter

    def run_repair(self, pairs=None, **kwargs):
        return repair.run_daily_repair(self.root, PAIRS if pairs is None else pairs,
            adapter_factory=self.factory, evidence_dir=Path(self.temp.name) / "evidence", **kwargs)

    def snapshot(self):
        return repair._snapshot(self.root, "TEST", DAY)

    def fingerprints(self):
        return repair._bank_fingerprints(self.root)

    def assert_replaced(self, before):
        after = self.snapshot()
        self.assertEqual(after["old"][4], 10.0)
        self.assertNotEqual(after["month_bytes"], before["month_bytes"])
        for day in ("2026-07-07", "2026-07-09"):
            self.assertEqual(after["dates"][day], before["dates"][day])
        record = after["manifest"]["daily_date_repairs"][-1]
        self.assertEqual(record["old"], repair._encoded_bar(before["old"]))
        self.assertEqual(record["new"], repair._encoded_bar(after["old"]))
        self.assertEqual(record["new_month_sha256"], repair._sha(after["month_bytes"]))
        self.assertFalse(after["stage"].exists())

    def test_r06_a_replaces_only_explicit_date_and_retains_evidence(self):
        before = self.snapshot()
        report = self.run_repair()
        self.assert_replaced(before)
        details = report["daily_repair"]
        repair._assert_untouched(details["before"], details["after"], details["allowed_changes"])
        self.assertEqual(len(self.raw.calls), 1)
        self.assertEqual(self.raw.calls[0][0], "bars")
        self.assertEqual(details["rows"][0]["status"], "replaced")
        self.assertTrue(Path(report["report_path"]).is_file())
        self.assertTrue(inspect_ledger(self.contexts[0].ledger.path, require_seal=True))

    def test_r06_a_inverse_ordinary_merge_does_not_satisfy_repair(self):
        before = self.snapshot()
        result = {"blocked_months": [], "dup_existing": 0, "conflicts": 0, "months": {}}
        replacement = (datetime(2026, 7, 8), 10.0, 11.0, 9.0, 10.0, 100)
        ib._commit_month_locked(self.root, "TEST", "1d", (2026, 7), [replacement],
            "inverse", "fake", result, lambda *args: None, conid=123)
        self.assertEqual(result["conflicts"], 1)
        self.assertEqual(self.snapshot()["month_bytes"], before["month_bytes"])
        with self.assertRaises(AssertionError):
            self.assert_replaced(before)

    def test_r06_b_unsettled_is_refused_before_connection(self):
        self.now = datetime(2026, 7, 8, 16, 19, tzinfo=NY)
        before = self.fingerprints()
        with self.assertRaises(repair.DailyRepairError):
            self.run_repair()
        self.assertEqual(self.fingerprints(), before)
        self.assertEqual(self.factory_calls, 0)
        self.assertEqual(self.raw.calls, [])

    def test_r06_b_inverse_bypassed_settlement_is_detected(self):
        self.now = datetime(2026, 7, 8, 16, 19, tzinfo=NY)
        # Deliberately corrupt the captured clock: the no-connection oracle must fail.
        with patch.object(self, "now", datetime(2026, 7, 8, 17, tzinfo=NY)):
            self.run_repair()
        with self.assertRaises(AssertionError):
            self.assertEqual(self.factory_calls, 0)

    def test_r06_c_wrong_date_is_refused(self):
        before = self.fingerprints()
        self.raw.rows = [bar(date(2026, 7, 9))]
        with self.assertRaises((repair.DailyRepairError, RequestRefused)):
            self.run_repair()
        self.assertEqual(self.fingerprints(), before)

    def test_r06_c_inverse_wrong_date_write_is_detected(self):
        before = self.snapshot()
        rows = list(before["bars"])
        rows[0] = (*rows[0][:4], 10.0, rows[0][5])
        with self.assertRaises(repair.DailyRepairError):
            repair._swap_locked(self.root, before, rows[0], "wrong-day")
        mutated = repair._date_manifest(rows)
        with self.assertRaises(AssertionError):
            self.assertEqual(before["dates"]["2026-07-07"], mutated["2026-07-07"])

    def crash_at(self, phase):
        def crash(observed):
            if observed == phase:
                raise InjectedCrash(phase)
        return patch.object(repair, "_fault", side_effect=crash)

    def test_r06_d_pre_swap_crashes_restore_original(self):
        for phase in ("prepared", "manifest_published"):
            with self.subTest(phase=phase):
                before = self.snapshot()
                with self.crash_at(phase), self.assertRaises(InjectedCrash):
                    self.run_repair()
                self.assertEqual(before["path"].read_bytes(), before["month_bytes"])
                outcome = repair.recover(self.root, PAIRS)
                self.assertEqual(outcome["rows"][0]["status"], "rolled_back")
                self.assertEqual(self.snapshot()["manifest_bytes"], before["manifest_bytes"])

    def test_r06_d_committed_swap_recovers(self):
        before = self.snapshot()
        with self.crash_at("month_published"), self.assertRaises(InjectedCrash):
            self.run_repair()
        result = repair.recover(self.root, PAIRS)
        self.assertEqual(result["rows"][0]["status"], "committed")
        self.assert_replaced(before)
        self.assertEqual(repair.recover(self.root, PAIRS)["rows"][0]["status"], "none")

    def test_r06_d_inverse_tampered_wal_kept_for_investigation(self):
        before = self.snapshot()
        with self.crash_at("prepared"), self.assertRaises(InjectedCrash):
            self.run_repair()
        marker = before["stage"] / repair._MARKER
        wal = repair._decode(marker.read_bytes())
        wal["month_after_sha256"] = "0" * 64
        storage._atomic_write_bytes(marker, repair._json(wal))
        with self.assertRaises(repair.DailyRepairError):
            repair.recover(self.root, PAIRS)
        self.assertTrue(marker.exists())
        self.assertEqual(before["path"].read_bytes(), before["month_bytes"])

    def test_cleanup_interruption_preserves_committed_disposition(self):
        before = self.snapshot()
        with self.crash_at("cleanup_payloads_removed"), self.assertRaises(InjectedCrash):
            self.run_repair()
        self.assertFalse((before["stage"] / repair._MARKER).exists())
        self.assertEqual(repair.recover(self.root, PAIRS)["rows"][0]["status"], "committed")
        self.assert_replaced(before)

    def test_identical_is_verified_without_bank_write(self):
        self.raw.rows[0].close = 9.5
        before = self.fingerprints()
        report = self.run_repair()
        self.assertEqual(report["daily_repair"]["rows"][0]["status"], "verified_identical")
        self.assertEqual(report["daily_repair"]["rows"][0]["writes"], 0)
        self.assertEqual(self.fingerprints(), before)
        self.assertEqual(len(self.raw.calls), 1)

    def test_identical_inverse_manifest_write_is_detected(self):
        before = self.fingerprints()
        manifest = storage.load_manifest(self.root / "TEST")
        storage.save_manifest(self.root / "TEST", manifest)
        with self.assertRaises(AssertionError):
            self.assertEqual(self.fingerprints(), before)

    def test_plan_is_read_only_and_has_no_fetch_context(self):
        before = {p.relative_to(self.root): p.read_bytes()
                  for p in self.root.rglob("*") if p.is_file()}
        result = repair.plan(self.root, PAIRS)
        self.assertEqual(result["pairs"][0]["stored"][4], 9.5)
        self.assertEqual(result["sends"], 0)
        self.assertEqual(self.contexts, [])
        self.assertEqual(before, {p.relative_to(self.root): p.read_bytes()
                                 for p in self.root.rglob("*") if p.is_file()})

    def test_explicit_input_validation(self):
        for pairs in ([], None, [PAIRS[0], PAIRS[0]], [{"ticker": "../TEST", "date": "2026-07-08"}],
                      [{"ticker": "TEST", "date": "20260708"}],
                      [{"ticker": "TEST", "date": "2026-07-08", "extra": True}]):
            with self.subTest(pairs=pairs), self.assertRaises((repair.DailyRepairError, storage.StorageError)):
                repair.explicit_pairs(pairs)
        for interval in ("1m", "1d-iv", "1h", None):
            with self.assertRaises(repair.DailyRepairError):
                repair.explicit_pairs(PAIRS, interval)

    def test_absent_date_and_twins_refused(self):
        with self.assertRaises(repair.DailyRepairError):
            self.run_repair([{"ticker": "TEST", "date": "2026-07-10"}])
        self.make_month("TEST", "1d", [7, 8, 9], fmt="parquet")
        with self.assertRaises(repair.DailyRepairError):
            self.run_repair()
        self.assertEqual(self.factory_calls, 0)

    def test_parquet_replacement_and_recovery(self):
        csv = self.snapshot()["path"]
        csv.unlink()  # Only this test's newly generated temporary fixture.
        self.make_month("TEST", "1d", [7, 8, 9], close=9.5, fmt="parquet")
        before = self.snapshot()
        with self.crash_at("month_published"), self.assertRaises(InjectedCrash):
            self.run_repair()
        repair.recover(self.root, PAIRS)
        self.assert_replaced(before)

    def test_invalid_prices_and_empty_response_preserve_original(self):
        before = self.fingerprints()
        bad = bar(DAY)
        bad.high = 1
        for response in ([], [bad], [bar(DAY), bar(DAY)]):
            self.raw.rows = response
            with self.subTest(response=response), self.assertRaises(Exception):
                self.run_repair()
            self.assertEqual(self.fingerprints(), before)

    def test_stale_snapshot_refuses_replacement(self):
        before = self.snapshot()
        manifest = deepcopy(before["manifest"])
        storage.save_manifest(before["tdir"], manifest)
        with self.assertRaisesRegex(repair.DailyRepairError, "stale"):
            repair._swap_locked(self.root, before,
                (datetime(2026, 7, 8), 10.0, 11.0, 9.0, 10.0, 100), "stale")
        self.assertEqual(before["path"].read_bytes(), before["month_bytes"])

    def test_pending_stage_blocks_plan_and_refetch(self):
        with self.crash_at("prepared"), self.assertRaises(InjectedCrash):
            self.run_repair()
        count = len(self.raw.calls)
        with self.assertRaises(repair.DailyRepairError):
            repair.plan(self.root, PAIRS)
        with self.assertRaises(repair.DailyRepairError):
            self.run_repair()
        self.assertEqual(len(self.raw.calls), count)

    def test_recovery_rejects_unselected_date(self):
        with self.crash_at("prepared"), self.assertRaises(InjectedCrash):
            self.run_repair()
        with self.assertRaises(repair.DailyRepairError):
            repair.recover(self.root, [{"ticker": "TEST", "date": "2026-07-09"}])

    def test_shared_helper_requires_owned_child(self):
        with self.assertRaises(RequestRefused):
            repair._repair_one(self.root, self.snapshot(), self.adapter, InstantPacer())
        self.assertEqual(self.raw.calls, [])

    def test_daily_repair_authority_cannot_widen_token_duration_or_producer(self):
        original = ib._fetch_request
        def checked(*args, **kwargs):
            context = repair.bridge.current_worker().context
            good = dict(variant="ibkr-bars", token="1d", duration="1 D")
            self.assertTrue(context.permits("ibkr.gap_fill.session_fetch", SimpleNamespace(**good)))
            for field, value in (("variant", "ibkr-head"), ("token", "1m"),
                                 ("token", "1d-iv"), ("duration", "2 D")):
                changed = dict(good, **{field: value})
                self.assertFalse(context.permits("ibkr.gap_fill.session_fetch",
                                                 SimpleNamespace(**changed)))
            with self.assertRaisesRegex(AuthorityError, "outside this operation purpose"):
                context.permits("ibkr.live_internal_daily_check.daily", SimpleNamespace(**good))
            return original(*args, **kwargs)
        with patch.object(ib, "_fetch_request", side_effect=checked):
            self.run_repair()
        self.assertEqual(len(self.raw.calls), 1)

    def test_early_close_repair_waits_for_settlement(self):
        day = date(2026, 11, 27)
        path = storage.month_file_path(self.root, "TEST", 2026, 11, "1d", fmt="csv")
        stats = storage.write_month_file(path,
            [(datetime(2026, 11, 27), 10.0, 11.0, 9.0, 9.5, 100)])
        manifest = storage.load_manifest(self.root / "TEST")
        storage.manifest_months(manifest, "1d")["2026-11"] = dict(stats, status="present")
        storage.save_manifest(self.root / "TEST", manifest)
        pairs = [{"ticker": "TEST", "date": day.isoformat()}]
        before = self.fingerprints()
        # Reviewed daily policy keeps the 16:20 floor even on a 13:00 close.
        self.assertEqual(self.authority.daily_settled_close(day).hour, 16)
        self.now = datetime(2026, 11, 27, 16, 19, tzinfo=NY)
        with self.assertRaises(repair.DailyRepairError):
            self.run_repair(pairs)
        self.assertEqual(self.fingerprints(), before)
        self.assertEqual(self.factory_calls, 0)
        self.now = datetime(2026, 11, 27, 16, 21, tzinfo=NY)
        self.raw.rows = [bar(day)]
        report = self.run_repair(pairs)
        self.assertEqual(report["daily_repair"]["rows"][0]["status"], "replaced")
        self.assertEqual(len(self.raw.calls), 1)

    def test_raw_adapter_call_without_authority_refused(self):
        with self.assertRaises(RequestRefused):
            self.adapter.fetch(ib_contract(), datetime(2026, 7, 8, 16), "1 D", "1 day")
        self.assertEqual(self.raw.calls, [])

    def test_non_target_fingerprint_inverse(self):
        before = self.fingerprints()
        after = dict(before)
        other = next(p for p in after if p.startswith("OTHER/"))
        after[other] = "0" * 64
        with self.assertRaises(repair.DailyRepairError):
            repair._assert_untouched(before, after, set())

    def test_tripwires_are_live(self):
        import socket
        with self.assertRaises(AuthorityError):
            socket.create_connection(("127.0.0.1", 7497))

    def test_two_selected_dates_share_month_without_lost_update(self):
        original = self.raw.reqHistoricalDataAsync
        def response(contract, **kwargs):
            self.raw.rows = [bar(date(2026, 7, 8 + len(self.raw.calls)))]
            return original(contract, **kwargs)
        self.raw.reqHistoricalDataAsync = response
        before = self.snapshot()
        report = self.run_repair(PAIRS + [{"ticker": "TEST", "date": "2026-07-09"}])
        after = self.snapshot()
        self.assertEqual(len(report["daily_repair"]["rows"]), 2)
        self.assertEqual(len(after["manifest"]["daily_date_repairs"]), 2)
        self.assertEqual(after["dates"]["2026-07-07"], before["dates"]["2026-07-07"])
        self.assertEqual([b[4] for b in after["bars"]], [9.5, 10.0, 10.0])

    def test_factory_foreign_change_refused_before_send(self):
        before = self.snapshot()
        def changed_factory():
            storage.save_manifest(before["tdir"], deepcopy(before["manifest"]))
            return self.adapter
        with patch.object(self, "factory", side_effect=changed_factory):
            with self.assertRaisesRegex(repair.DailyRepairError, "batch preflight"):
                self.run_repair()
        self.assertEqual(self.raw.calls, [])
        self.assertEqual(before["path"].read_bytes(), before["month_bytes"])

    def test_repair_history_survives_ordinary_daily_append(self):
        stale = deepcopy(self.snapshot()["manifest"])
        self.run_repair()
        history = deepcopy(self.snapshot()["manifest"]["daily_date_repairs"])
        result = {"blocked_months": [], "dup_existing": 0, "conflicts": 0,
                  "months": {}, "added": 0, "written": 0}
        ib._commit_month_locked(self.root, "TEST", "1d", (2026, 7),
            [(datetime(2026, 7, 10), 10.0, 11.0, 9.0, 10.0, 100)],
            "append", "fake", result, lambda *args: None, conid=123,
            mstate={"manifest": stale, "since_save": ib.MANIFEST_CHECKPOINT_MONTHS})
        self.assertEqual(result["written"], 1)
        self.assertEqual(storage.load_manifest(self.root / "TEST")["daily_date_repairs"], history)

    def test_existing_correction_stage_blocks_ordinary_writer(self):
        before = self.snapshot()
        with self.crash_at("prepared"), self.assertRaises(InjectedCrash):
            self.run_repair()
        result = {"blocked_months": []}
        ib._commit_month_locked(self.root, "TEST", "1d", (2026, 7),
            [(datetime(2026, 7, 10), 10.0, 11.0, 9.0, 10.0, 100)],
            "append", "fake", result, lambda *args: None)
        self.assertEqual(len(result["blocked_months"]), 1)
        self.assertEqual(before["path"].read_bytes(), before["month_bytes"])

    def test_foreign_manifest_during_recovery_is_retained(self):
        before = self.snapshot()
        with self.crash_at("prepared"), self.assertRaises(InjectedCrash):
            self.run_repair()
        storage.save_manifest(before["tdir"], deepcopy(before["manifest"]))
        with self.assertRaisesRegex(repair.DailyRepairError, "foreign manifest"):
            repair.recover(self.root, PAIRS)
        self.assertTrue(before["stage"].is_dir())
        self.assertEqual(before["path"].read_bytes(), before["month_bytes"])

    def test_recovery_rejects_hash_consistent_wrong_record(self):
        import base64
        before = self.snapshot()
        with self.crash_at("prepared"), self.assertRaises(InjectedCrash):
            self.run_repair()
        marker = before["stage"] / repair._MARKER
        wal = repair._decode(marker.read_bytes())
        manifest = repair._decode(base64.b64decode(wal["manifest_after"]))
        manifest["daily_date_repairs"][-1]["old_day_sha256"] = "0" * 64
        payload = repair._json(manifest)
        wal["manifest_after"] = base64.b64encode(payload).decode("ascii")
        wal["manifest_after_sha256"] = repair._sha(payload)
        storage._atomic_write_bytes(marker, repair._json(wal))
        with self.assertRaisesRegex(repair.DailyRepairError, "correction record"):
            repair.recover(self.root, PAIRS)
        self.assertTrue(marker.exists())

    def test_cancel_before_send_leaves_old_values(self):
        import threading
        cancel = threading.Event()
        cancel.set()
        before = self.fingerprints()
        with self.assertRaises(ib.Cancelled):
            self.run_repair(cancel=cancel)
        self.assertEqual(self.raw.calls, [])
        self.assertEqual(self.fingerprints(), before)


def _crash_parent(path):
    root = Path(__file__).resolve().parents[1] / "Run Logs"
    supplied = Path(path).resolve(strict=True)
    if (supplied.parent != root.resolve() or not supplied.name.startswith("daily-repair-crash-")
            or supplied.is_symlink() or supplied.is_junction()
            or (supplied / "fixture-owner.txt").read_bytes() != b"A182 synthetic crash fixture\n"):
        raise ValueError("not an owned synthetic crash fixture")
    return supplied


def crash_child(phase, parent):
    if phase not in CRASH_PHASES:
        raise ValueError("unknown crash phase")
    parent = _crash_parent(parent)
    if set(p.name for p in parent.iterdir()) != {"fixture-owner.txt"}:
        raise ValueError("crash fixture must be fresh")
    DailyRepair.setUpClass()
    case = DailyRepair("test_r06_a_replaces_only_explicit_date_and_retains_evidence")
    case.crash_parent = parent
    case.setUp()
    def hard_exit(observed):
        if observed == phase:
            os._exit(73)
    repair._fault = hard_exit
    case.run_repair()
    raise AssertionError("requested hard exit was not reached")


def recover_child(parent):
    parent = _crash_parent(parent)
    children = list(parent.glob("daily-repair-synthetic-*"))
    if len(children) != 1 or children[0].is_symlink() or children[0].is_junction():
        raise ValueError("ambiguous synthetic child")
    bank = children[0] / "synthetic bank"
    with policy.offline_transports(), patch.object(repair.operation_gate, "acquire",
                                                   side_effect=lambda *a, **k: OfflineLease()):
        outcome = repair.recover(bank, PAIRS)
        snap = repair._snapshot(bank, "TEST", DAY)
        outcome["close"] = snap["old"][4]
        outcome["history_count"] = len(snap["manifest"].get("daily_date_repairs", []))
        outcome["pending_stage"] = snap["stage"].exists()
        print(json.dumps(outcome), flush=True)


class HardExit(unittest.TestCase):
    def test_process_death_and_fresh_process_recovery(self):
        # Parent launches only this file's fixed synthetic worker. Each child
        # installs tripwires before production imports and every repair/recovery.
        parent_root = Path(__file__).resolve().parents[1] / "Run Logs"
        parent_root.mkdir(exist_ok=True)
        for phase in CRASH_PHASES:
            with self.subTest(phase=phase), tempfile.TemporaryDirectory(
                    prefix="daily-repair-crash-", dir=parent_root) as directory:
                parent = Path(directory)
                (parent / "fixture-owner.txt").write_bytes(b"A182 synthetic crash fixture\n")
                base = [sys.executable, "-B", str(Path(__file__).resolve())]
                result = subprocess.run(base + ["--crash-child", phase, str(parent)],
                    capture_output=True, text=True, timeout=40)
                self.assertEqual(result.returncode, 73, result.stdout + result.stderr)
                recovered = subprocess.run(base + ["--recover-child", str(parent)],
                    capture_output=True, text=True, timeout=40)
                self.assertEqual(recovered.returncode, 0, recovered.stdout + recovered.stderr)
                data = json.loads(recovered.stdout)
                committed = phase in {"month_published", "cleanup_payloads_removed", "cleanup_stage_detached"}
                detached = phase in {"preparation_unpublished", "cleanup_stage_detached"}
                self.assertEqual(data["rows"][0]["status"],
                    "none" if detached else "committed" if committed else "rolled_back")
                self.assertEqual(data["close"], 10.0 if committed else 9.5)
                self.assertEqual(data["history_count"], 1 if committed else 0)
                self.assertFalse(data["pending_stage"])


if __name__ == "__main__":
    if len(sys.argv) == 4 and sys.argv[1] == "--crash-child":
        crash_child(sys.argv[2], sys.argv[3])
    elif len(sys.argv) == 3 and sys.argv[1] == "--recover-child":
        recover_child(sys.argv[2])
    else:
        result = unittest.main(verbosity=2, exit=False).result
        print(f"{result.testsRun} checks, {len(result.errors) + len(result.failures)} failed")
        raise SystemExit(not result.wasSuccessful())
