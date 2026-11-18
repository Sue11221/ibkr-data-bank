"""Deterministic safety tests for the WS8 report-only live spot probe.

Fixture banks are the only banks written. Request tests inject raw transport
through the existing confined Operations owner and execute the shipped guarded
adapter. The suite never contacts IBKR, HTTP, sockets, or child processes.
"""

from __future__ import annotations

import ast
import builtins
import contextlib
import datetime as dt
import hashlib
import io
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import types
import urllib.request
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

def main():
    # Delegate before production imports. Only the existing registered suite
    # installs tripwires/mints policy; this legacy module is not a new owner.
    import unittest
    from fetch_a2_ibkr_workflows_selftest import Operations
    result = unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite([
        Operations("test_legacy_probe_contracts_through_confined_transport")]))
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())


# The confined owner imports this module inside its live transport guard.
import live_spot_probe as probe  # noqa: E402
import live_spot_probe_cli as cli  # noqa: E402
import live_spot_probe_reference as reference  # noqa: E402
import operation_gate  # noqa: E402
import stock_storage as storage  # noqa: E402
from fetch_authority import AuthorityError  # noqa: E402
from fetch_ledger import LedgerError  # noqa: E402
from fetch_run_context import RequestCancelled  # noqa: E402


FIXED_NOW = dt.datetime(2026, 7, 13, 16, 30, tzinfo=dt.timezone.utc)
FAILS = []
TOTAL = [0]


def _physical_fetch(*args, **kwargs):
    raise AssertionError("physical tests require the existing confined Operations fixture")


def _reusable_evidence(*args, **kwargs):
    raise AssertionError("reuse tests require authentic evidence from the confined Operations fixture")


def check(name, condition, detail=""):
    TOTAL[0] += 1
    ok = bool(condition)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}"
          + (f" -- {detail}" if detail and not ok else ""))
    if not ok:
        FAILS.append(name)


def expect_error(name, exc_type, fn):
    try:
        fn()
    except exc_type:
        check(name, True)
    except Exception as exc:  # noqa: BLE001
        check(name, False, f"got {type(exc).__name__}: {exc}")
    else:
        check(name, False, "no exception")


def minute_bars(day, *, base=100.0, count=12):
    start = dt.datetime.combine(day, dt.time(9, 30))
    return [
        (start + dt.timedelta(minutes=index),
         base + index * 0.01,
         base + index * 0.01,
         base + index * 0.01,
         base + index * 0.01,
         1000 + index)
        for index in range(count)
    ]


def daily_bars(days, *, base=100.0, interval="1d", values=None):
    days = list(days)
    if values is None:
        closes = [base + index for index in range(len(days))]
    else:
        closes = list(values)
        if len(closes) != len(days):
            raise ValueError("daily fixture values must match its days")
    ratio_kind = storage.kind_of(interval) in storage.RATIO_KINDS
    return [
        (dt.datetime.combine(day, dt.time()),
         close, close, close, close,
         0 if ratio_kind else 1000 + index)
        for index, (day, close) in enumerate(zip(days, closes))
    ]


def seed_series(root, ticker, conid, days, *, actions=None,
                corrections=None, verified_absent=None):
    root = Path(root)
    ticker_dir = root / ticker
    ticker_dir.mkdir(parents=True, exist_ok=True)
    manifest = storage.new_manifest(ticker, ticker)
    manifest["conid"] = conid
    if actions is not None:
        manifest["actions"] = actions
    if corrections is not None:
        manifest["data_corrections"] = corrections
    months = storage.manifest_months(manifest, "1m")
    grouped = {}
    for day in days:
        grouped.setdefault(day.strftime("%Y-%m"), []).extend(minute_bars(day))
    for month, bars in sorted(grouped.items()):
        year, number = int(month[:4]), int(month[5:7])
        path = storage.month_file_path(root, ticker, year, number, "1m")
        months[month] = storage.write_month_file(path, bars)
    if verified_absent is not None:
        manifest["intervals"]["1m"]["verified_absent"] = verified_absent
    storage.save_manifest(ticker_dir, manifest)
    return manifest


def seed_daily_series(root, ticker, conid, days, *, actions=None,
                      corrections=None, verified_absent=None, base=100.0,
                      interval="1d", values=None):
    root = Path(root)
    ticker_dir = root / ticker
    ticker_dir.mkdir(parents=True, exist_ok=True)
    manifest = storage.new_manifest(ticker, ticker)
    manifest["conid"] = conid
    if actions is not None:
        manifest["actions"] = actions
    if corrections is not None:
        manifest["data_corrections"] = corrections
    grouped = {}
    for bar in daily_bars(
            days, base=base, interval=interval, values=values):
        grouped.setdefault(bar[0].strftime("%Y-%m"), []).append(bar)
    months = storage.manifest_months(manifest, interval)
    for month, bars in sorted(grouped.items()):
        year, number = int(month[:4]), int(month[5:7])
        path = storage.month_file_path(root, ticker, year, number, interval)
        months[month] = storage.write_month_file(path, bars)
    if verified_absent is not None:
        manifest["intervals"][interval]["verified_absent"] = verified_absent
    storage.save_manifest(ticker_dir, manifest)
    return manifest


def seed_daily_only(root, ticker, conid):
    root = Path(root)
    ticker_dir = root / ticker
    ticker_dir.mkdir(parents=True, exist_ok=True)
    manifest = storage.new_manifest(ticker, ticker)
    manifest["conid"] = conid
    day = dt.date(2026, 6, 1)
    stamp = dt.datetime.combine(day, dt.time())
    bars = [(stamp, 10.0, 10.0, 10.0, 10.0, 100)]
    path = storage.month_file_path(root, ticker, 2026, 6, "1d")
    storage.manifest_months(manifest, "1d")["2026-06"] = (
        storage.write_month_file(path, bars))
    storage.save_manifest(ticker_dir, manifest)


def tree_snapshot(root):
    root = Path(root)
    out = {}
    for path in [root] + sorted(root.rglob("*"), key=lambda item: str(item)):
        stat = path.stat()
        relative = "." if path == root else path.relative_to(root).as_posix()
        out[relative] = {
            "dir": path.is_dir(),
            "size": stat.st_size,
            "mtime_ns": stat.st_mtime_ns,
            "sha256": (None if path.is_dir() else hashlib.sha256(
                path.read_bytes()).hexdigest()),
        }
    return out


@contextlib.contextmanager
def deny_writes_network_processes(*, transport=True):
    originals = []

    def patch(owner, name, replacement):
        originals.append((owner, name, getattr(owner, name)))
        setattr(owner, name, replacement)

    def bomb(*_args, **_kwargs):
        raise AssertionError("forbidden write/network/process API called")

    def guarded_open(fn):
        def wrapper(file, mode="r", *args, **kwargs):
            if any(flag in str(mode) for flag in "wax+"):
                return bomb(file, mode)
            return fn(file, mode, *args, **kwargs)
        return wrapper

    patch(builtins, "open", guarded_open(builtins.open))
    patch(io, "open", guarded_open(io.open))
    original_os_open = os.open

    def guarded_os_open(path, flags, *args, **kwargs):
        write_flags = (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC
                       | os.O_APPEND)
        if int(flags) & write_flags:
            return bomb(path, flags)
        return original_os_open(path, flags, *args, **kwargs)

    patch(os, "open", guarded_os_open)
    for name in (
            "rename", "replace", "remove", "unlink", "mkdir", "makedirs",
            "rmdir", "removedirs", "link", "symlink"):
        if hasattr(os, name):
            patch(os, name, bomb)
    for name in (
            "write_text", "write_bytes", "touch", "mkdir", "rename",
            "replace", "unlink", "rmdir", "symlink_to", "hardlink_to"):
        if hasattr(Path, name):
            patch(Path, name, bomb)
    for name in (
            "copy", "copy2", "copyfile", "copytree", "move", "rmtree"):
        patch(shutil, name, bomb)
    if transport:
        patch(socket, "socket", bomb)
        patch(socket, "create_connection", bomb)
        patch(urllib.request, "urlopen", bomb)
        for name in ("Popen", "run", "call", "check_call", "check_output"):
            patch(subprocess, name, bomb)
        patch(os, "system", bomb)
    try:
        yield
    finally:
        for owner, name, value in reversed(originals):
            setattr(owner, name, value)


class FakeLiveFetcher:
    def __init__(self, result_fn=None, *, start_error=None, fetch_error=None):
        self.result_fn = result_fn or expected_live_result
        self.start_error = start_error
        self.fetch_error = fetch_error
        self.start_calls = 0
        self.close_calls = 0
        self.calls = []

    def start(self):
        self.start_calls += 1
        if self.start_error is not None:
            raise self.start_error

    def fetch_day(self, snapshot):
        self.calls.append((snapshot["ticker"], snapshot["day"]))
        if self.fetch_error is not None:
            raise self.fetch_error
        return self.result_fn(snapshot)

    def close(self):
        self.close_calls += 1


class FakeDailySpanFetcher:
    def __init__(self, result_fn=None, *, start_error=None, fetch_error=None):
        self.result_fn = result_fn or expected_daily_live_result
        self.start_error = start_error
        self.fetch_error = fetch_error
        self.start_calls = 0
        self.close_calls = 0
        self.calls = []

    def start(self):
        self.start_calls += 1
        if self.start_error is not None:
            raise self.start_error

    def fetch_span(self, snapshot):
        self.calls.append((
            snapshot["ticker"], snapshot["span_start"],
            snapshot["span_end"]))
        if self.fetch_error is not None:
            raise self.fetch_error
        return self.result_fn(snapshot)

    def close(self):
        self.close_calls += 1


def expected_live_result(snapshot):
    if snapshot["day"] in snapshot["verified_absent"]:
        closes = {}
    else:
        factor = snapshot["ledger_factor"]
        correction = snapshot.get("correction")
        if correction and correction["mode"] == "expected_divergence":
            factor /= correction["factor"]
        closes = {
            key: value * factor
            for key, value in snapshot["stored"].items()
        }
    return {
        "closes": closes,
        "volume": 0 if not closes else snapshot["stored_volume"],
        "raw_bar_count": len(closes),
        "accepted_bar_count": len(closes),
        "discarded": {"invalid": 0, "outside_day": 0, "non_rth": 0},
        "port": 2000,
        "client_id": 7315,
    }


def expected_daily_live_result(snapshot):
    closes = {}
    for context in snapshot["contexts"]:
        correction = context.get("correction")
        if correction and correction.get("mode") == "expected_absent":
            continue
        factor = context["ledger_factor"]
        if correction and correction.get("mode") == "expected_divergence":
            factor /= correction["factor"]
        for day, value in context["stored"].items():
            closes[day] = value * factor
    return {
        "closes": closes,
        "volume": snapshot["stored_volume"] if closes else 0.0,
        "raw_bar_count": len(closes),
        "accepted_bar_count": len(closes),
        "discarded": {"invalid": 0, "outside_day": 0, "non_rth": 0},
        "port": 2000,
        "client_id": 7315,
    }


def reference_semantics():
    base = reference._day()
    cases = [
        ("identical day", base, dict(base), 1.0, None, "MATCH"),
        ("CRWD ledger", base, {key: value * 0.25
                               for key, value in base.items()},
         0.25, None, "MATCH"),
        ("unrecorded basis", base, {key: value * 0.25
                                    for key, value in base.items()},
         1.0, None, "BASIS_STEP"),
        ("FTNT correction", base, {key: value / 4.0
                                    for key, value in base.items()},
         1.0, 4.0, "DOCUMENTED_DIVERGENCE"),
    ]
    for name, stored, live, ledger, correction, expected in cases:
        result = probe.classify_day(
            stored, live, ledger_factor=ledger,
            correction_factor=correction)
        oracle = reference.classify_day(
            stored, live, ledger_factor=ledger,
            correction_factor=correction)
        check(f"reference: {name} -> {expected}",
              result["verdict"] == oracle["verdict"] == expected,
              str(result))

    keys = sorted(base)
    revised = dict(base)
    for key in keys[::30]:
        revised[key] *= 1.04
    check("reference: 13 revised bars -> SCATTERED_MISMATCH",
          probe.classify_day(base, revised)["verdict"]
          == "SCATTERED_MISMATCH")
    one = dict(base)
    one[keys[5]] *= 1.02
    check("reference: one revised bar -> MATCH",
          probe.classify_day(base, one)["verdict"] == "MATCH")
    half = {key: base[key] for key in keys[:150]}
    check("reference: half coverage -> COVERAGE_MISMATCH",
          probe.classify_day(base, half)["verdict"]
          == "COVERAGE_MISMATCH")
    check("reference: empty live -> NO_LIVE_DATA",
          probe.classify_day(base, {})["verdict"] == "NO_LIVE_DATA")
    first = probe.pick_probe(["2015-03", "2019-11", "2023-06"], 42)
    second = probe.pick_probe(["2015-03", "2019-11", "2023-06"], 42)
    other = probe.pick_probe(["2015-03", "2019-11", "2023-06"], 43)
    check("reference: probe pick is seed deterministic", first == second)
    check("reference: different seed returns a valid month",
          other[0] in {"2015-03", "2019-11", "2023-06"})

    combined_live = {key: value * 0.25 / 4.0
                     for key, value in base.items()}
    combined = probe.classify_day(
        base, combined_live, ledger_factor=0.25, correction_factor=4.0)
    check("classifier combines later ledger and documented correction",
          combined["verdict"] == "DOCUMENTED_DIVERGENCE",
          str(combined))


def fixture_semantics(tmp):
    bank = Path(tmp) / "bank"
    cli_bank = Path(tmp) / "cli-bank"
    run_logs = Path(tmp) / "run-logs"
    bank.mkdir()
    cli_bank.mkdir()
    run_logs.mkdir()

    action = {
        "date": "2026-06-29",
        "kind": "split",
        "factor": 0.25,
        "applies": "price",
        "source": "measured",
        "evidence": "fixture",
        "run": "selftest",
    }
    ftnt_note = {
        "type": "phantom_split_correction",
        "ticker": "FTNT",
        "ex_date": "2014-01-13",
        "factor": 4,
        "intervals": ["1d", "1m"],
        "applied": "2026-07-09T12:08:20-04:00",
    }
    wbd_note = {
        "type": "identity_truncation",
        "ticker": "WBD",
        "cutover": "2022-04-11",
        "intervals": ["1d", "1m"],
        "applied": "2026-07-09T14:49:10-04:00",
    }
    seed_series(bank, "AAA", 101, [
        dt.date(2026, 5, 4), dt.date(2026, 5, 5),
        dt.date(2026, 6, 1)], verified_absent=["2026-05-06"])
    seed_series(bank, "BBB", 102, [
        dt.date(2026, 4, 1), dt.date(2026, 5, 1)])
    seed_series(bank, "CRWD", 103, [dt.date(2026, 6, 26)],
                actions=[action])
    seed_series(bank, "FTNT", 104, [dt.date(2013, 12, 20)],
                corrections=[ftnt_note])
    seed_series(bank, "WBD", 105, [dt.date(2021, 3, 30)],
                corrections=[wbd_note])
    seed_daily_only(bank, "DAILY", 106)
    (bank / "_quarantine" / "QQQ").mkdir(parents=True)
    (bank / "_quarantine" / "QQQ" / "manifest.json").write_text(
        "{}", encoding="utf-8")
    seed_series(cli_bank, "AAA", 301, [dt.date(2026, 5, 4)])
    seed_series(cli_bank, "BBB", 302, [dt.date(2026, 5, 5)])

    candidates, errors, overflow = probe.discover_candidates(bank)
    check("selection: only eligible direct 1m tickers are candidates",
          [item["ticker"] for item in candidates]
          == ["AAA", "BBB", "CRWD", "FTNT", "WBD"],
          str(candidates))
    check("selection: clean discovery has no errors",
          not errors and overflow == 0, str(errors))

    crwd = probe.stored_month_snapshot(bank, "CRWD", "2026-06")
    check("snapshot: CRWD uses shared adjustment_factor semantics",
          crwd["ledger_factor"] == 0.25
          and crwd["basis_actions_applied"][0]["date"] == "2026-06-29",
          str(crwd))
    check("snapshot: evidence exposes SHA-gated digests, not bar payload",
          len(crwd["stored_digest"]) == 64
          and crwd["month_sha256"]
          and crwd["network"] is False and crwd["written"] is False)

    ftnt = probe.stored_month_snapshot(bank, "FTNT", "2013-12")
    check("snapshot: FTNT correction region carries factor four",
          ftnt["correction"]["mode"] == "expected_divergence"
          and ftnt["correction"]["factor"] == 4.0,
          str(ftnt["correction"]))
    live_corrected = {key: value / 4.0
                      for key, value in ftnt["stored"].items()}
    holding = probe.classify_probe(
        ftnt["stored"], live_corrected, day=ftnt["day"],
        ledger_factor=ftnt["ledger_factor"],
        correction=ftnt["correction"],
        verified_absent=ftnt["verified_absent"])
    check("correction guard: documented divergence is pass-equivalent",
          holding["verdict"] == "DOCUMENTED_DIVERGENCE"
          and holding["safety_status"] == "PASS"
          and holding["pass_equivalent"] is True,
          str(holding))
    rebroken = probe.classify_probe(
        ftnt["stored"], dict(ftnt["stored"]), day=ftnt["day"],
        correction=ftnt["correction"])
    check("correction guard: repair drift is a loud REGRESSION",
          rebroken["verdict"] == "MATCH"
          and rebroken["safety_status"] == "REGRESSION"
          and rebroken["needs_human"] is True
          and rebroken["remedy"] == "correction_regression_review",
          str(rebroken))
    partial_live = dict(list(live_corrected.items())[:3])
    inconclusive = probe.classify_probe(
        ftnt["stored"], partial_live, day=ftnt["day"],
        correction=ftnt["correction"])
    check("correction guard: insufficient coverage is review, not false regression",
          inconclusive["verdict"] == "COVERAGE_MISMATCH"
          and inconclusive["safety_status"] == "NEEDS_HUMAN",
          str(inconclusive))

    wbd = probe.stored_month_snapshot(bank, "WBD", "2021-03")
    check("correction guard: reappearing truncated identity is REGRESSION",
          wbd["correction"]["mode"] == "expected_absent"
          and wbd["offline_safety_status"] == "REGRESSION"
          and wbd["needs_human"] is True,
          str(wbd))

    expected_absence = probe.classify_probe(
        crwd["stored"], {}, day=crwd["day"],
        verified_absent=[crwd["day"]])
    check("NO_LIVE_DATA: verified absent is pass-equivalent",
          expected_absence["safety_status"] == "EXPECTED_SOURCE_ABSENCE"
          and expected_absence["source_absent"] is True
          and expected_absence["pass_equivalent"] is True,
          str(expected_absence))
    unknown_absence = probe.classify_probe(
        crwd["stored"], {}, day=crwd["day"], verified_absent=[])
    check("NO_LIVE_DATA: unknown absence becomes coverage question",
          unknown_absence["safety_status"] == "COVERAGE_QUESTION"
          and unknown_absence["needs_human"] is True
          and unknown_absence["remedy"] == "coverage_review",
          str(unknown_absence))
    expect_error(
        "correction guard: malformed correction evidence fails closed",
        probe.EvidenceError,
        lambda: probe.classify_probe(
            crwd["stored"], crwd["stored"], day=crwd["day"],
            correction=["not", "an", "object"]))
    expect_error(
        "correction guard: unknown correction mode fails closed",
        probe.EvidenceError,
        lambda: probe.classify_probe(
            crwd["stored"], crwd["stored"], day=crwd["day"],
            correction={"mode": "ignore", "records": [{}]}))
    expect_error(
        "correction guard: factorless divergence fails closed",
        probe.EvidenceError,
        lambda: probe.classify_probe(
            crwd["stored"], crwd["stored"], day=crwd["day"],
            correction={
                "mode": "expected_divergence", "records": [{}]}))

    plan_a = probe.build_plan(bank, seed=42, count=4, now=FIXED_NOW)
    plan_b = probe.build_plan(bank, seed=42, count=4, now=FIXED_NOW)
    check("plan: same seed and bank produce byte-identical JSON",
          probe._canonical_json(plan_a) == probe._canonical_json(plan_b))
    check("plan: all rows are bounded evidence summaries",
          all("stored" not in row and row["stored_bars"] > 0
              and len(row["stored_digest"]) == 64
              for row in plan_a["probes"]), str(plan_a))
    check("plan: seed and bank fingerprint are explicit",
          plan_a["seed"] == 42
          and len(plan_a["candidate_fingerprint"]) == 64)
    check("plan: Checkpoint A explicitly has no live capability",
          plan_a["network"] is False
          and plan_a["bank_written"] is False
          and plan_a["artifact_written"] is False
          and plan_a["live_capability"] is False
          and plan_a["request_count"] == 0
          and all(row["request_count"] == 0 for row in plan_a["probes"]))
    wbd_plan = probe.build_plan(bank, seed=1, count=5, now=FIXED_NOW)
    check("plan: correction regression elevates top-level status",
          wbd_plan["status"] == "review_required"
          and wbd_plan["offline_regression_count"] == 1,
          str(wbd_plan))
    check("plan: default seed is the UTC calendar date",
          probe.build_plan(bank, count=1, now=FIXED_NOW)["seed"] == 20260713)

    before = tree_snapshot(bank)
    trapped = None
    try:
        with deny_writes_network_processes():
            trapped = probe.build_plan(bank, seed=7, count=3, now=FIXED_NOW)
    except Exception as exc:  # noqa: BLE001
        check("purity: offline plan passes write/network/process traps",
              False, f"{type(exc).__name__}: {exc}")
    else:
        check("purity: offline plan passes write/network/process traps",
              trapped["selected_count"] == 3)
    check("purity: complete fixture bank bytes and metadata are unchanged",
          before == tree_snapshot(bank))

    expect_error(
        "race: stale manifest fingerprint fails closed",
        probe.EvidenceError,
        lambda: probe.stored_month_snapshot(
            bank, "AAA", "2026-05",
            expected_manifest_fingerprint="0" * 64))

    target = probe.artifact_path(
        "codex", run_logs_root=run_logs, now=FIXED_NOW)
    bank_before_artifact = tree_snapshot(bank)
    written = probe.write_artifact(
        target, plan_a, bank_root=bank, run_logs_root=run_logs)
    payload = json.loads(written.read_text(encoding="utf-8"))
    check("artifact: direct Run Logs child is written atomically",
          written.parent == run_logs.resolve()
          and payload["artifact_written"] is True
          and payload["bank_written"] is False)
    check("artifact: writing outside the bank leaves bank unchanged",
          tree_snapshot(bank) == bank_before_artifact)
    expect_error(
        "artifact: path inside bank is refused",
        probe.EvidenceError,
        lambda: probe.write_artifact(
            bank / "bad.json", plan_a,
            bank_root=bank, run_logs_root=run_logs))
    expect_error(
        "artifact: nested Run Logs path is refused",
        probe.EvidenceError,
        lambda: probe.write_artifact(
            run_logs / "nested" / "bad.json", plan_a,
            bank_root=bank, run_logs_root=run_logs))
    expect_error(
        "artifact: traversal owner is refused",
        probe.LiveSpotProbeError,
        lambda: probe.artifact_path(
            "../bad", run_logs_root=run_logs, now=FIXED_NOW))

    minute_default_out = io.StringIO()
    with contextlib.redirect_stdout(minute_default_out):
        minute_default_code = cli.main(
            ["plan", "--seed", "1", "--count", "1"],
            root=bank, now=FIXED_NOW)
    minute_explicit_out = io.StringIO()
    with contextlib.redirect_stdout(minute_explicit_out):
        minute_explicit_code = cli.main(
            ["plan", "--interval", "1m", "--seed", "1", "--count", "1"],
            root=bank, now=FIXED_NOW)
    check("daily compatibility: CLI default and explicit 1m are identical",
          minute_default_code == minute_explicit_code == 0
          and minute_default_out.getvalue() == minute_explicit_out.getvalue())

    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        code = cli.main(
            ["plan", "--seed", "42", "--count", "2"],
            root=cli_bank, now=FIXED_NOW)
    envelope = json.loads(out.getvalue())
    check("CLI: explicit plan returns one JSON success envelope",
          code == 0 and envelope["kind"] == "live_spot_probe_plan"
          and envelope["selected_count"] == 2)

    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        code = cli.main(
            ["plan", "--count", "2"], root=cli_bank, now=FIXED_NOW)
    envelope = json.loads(out.getvalue())
    check("CLI: omitted seed uses logged date seed",
          code == 0 and envelope["seed"] == 20260713)

    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        code = cli.main(
            ["plan", "--count", "0"], root=bank, now=FIXED_NOW)
    error = json.loads(out.getvalue())
    check("CLI: invalid count is bounded JSON with deterministic exit 2",
          code == 2 and error["kind"] == "live_spot_probe_command_error"
          and "traceback" not in out.getvalue().lower())

    blocked_fetcher = FakeLiveFetcher()
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        code = cli.main(
            ["probe", "--ticker", "AAA", "--day", "2026-05-05"],
            root=bank, now=FIXED_NOW, live_fetcher=blocked_fetcher,
            gate_path=Path(tmp) / "blocked.lock", run_logs_root=run_logs)
    error = json.loads(out.getvalue())
    check("CLI: live command requires explicit --allow-live before any fetch",
          code == 2 and error["live_capability"] is True
          and error["live_authorized"] is False
          and error["network"] is False and not blocked_fetcher.calls)

    for terminal_type in (AuthorityError, LedgerError, RequestCancelled):
        terminal_fetcher = FakeLiveFetcher()
        out = io.StringIO()
        with patch.object(
                probe, "run_live_probe",
                side_effect=terminal_type("terminal witness")) as root_call, \
                contextlib.redirect_stdout(out):
            code = cli.main(
                ["probe", "--allow-live", "--ticker", "AAA",
                 "--day", "2026-05-05"], root=bank, now=FIXED_NOW,
                live_fetcher=terminal_fetcher,
                gate_path=Path(tmp) / "terminal.lock",
                run_logs_root=run_logs)
        envelope = json.loads(out.getvalue())
        check(f"CLI: {terminal_type.__name__} keeps typed JSON terminal exit",
              code == 2 and root_call.call_count == 1
              and envelope["kind"] == "live_spot_probe_command_error"
              and envelope["error"] == {
                  "type": terminal_type.__name__,
                  "message": "terminal witness"}
              and envelope["live_capability"] is True
              and envelope["live_authorized"] is True
              and envelope["network"] is False
              and envelope["written"] is False
              and envelope["bank_written"] is False
              and not terminal_fetcher.calls
              and terminal_fetcher.start_calls == 0)

    live_path_semantics(bank, run_logs, tmp)
    embedded_path_semantics(bank, run_logs, tmp)

    return bank


def live_path_semantics(bank, run_logs, tmp):
    bank = Path(bank)
    run_logs = Path(run_logs)
    gate = Path(tmp) / "live-probe.lock"
    full_before = tree_snapshot(bank)

    exact = probe.stored_month_snapshot(
        bank, "AAA", "2026-05", day="2026-05-05")
    check("live selection: exact stored day is selected without index drift",
          exact["day"] == "2026-05-05")
    expect_error(
        "live selection: unstored explicit day fails before network",
        probe.EvidenceError,
        lambda: probe.stored_month_snapshot(
            bank, "AAA", "2026-05", day="2026-05-06"))

    unsafe = FakeLiveFetcher()
    expect_error(
        "live safety: operation gate inside bank is refused before setup",
        probe.EvidenceError,
        lambda: probe.run_live_probe(
            bank, ticker="AAA", day="2026-05-05", owner="unsafe-gate",
            now=FIXED_NOW, live_fetcher=unsafe,
            gate_path=bank / ".unsafe.lock", run_logs_root=run_logs))
    expect_error(
        "live safety: artifact root inside bank is refused before setup",
        probe.EvidenceError,
        lambda: probe.run_live_probe(
            bank, ticker="AAA", day="2026-05-05", owner="unsafe-log",
            now=FIXED_NOW, live_fetcher=unsafe, gate_path=gate,
            run_logs_root=bank))
    check("live safety: refused paths create no bank entry or fetch",
          not unsafe.calls and unsafe.start_calls == 0
          and tree_snapshot(bank) == full_before)

    clean_fetcher = FakeLiveFetcher()
    clean = probe.run_live_probe(
        bank, ticker="AAA", day="2026-05-05", owner="clean",
        now=FIXED_NOW, live_fetcher=clean_fetcher, gate_path=gate,
        run_logs_root=run_logs)
    clean_row = clean["probes"][0]
    check("live run: clean explicit day is a complete MATCH",
          clean["status"] == "complete"
          and clean_row["verdict"] == "MATCH"
          and clean_row["safety_status"] == "PASS",
          str(clean_row))
    standalone_snapshot = probe.stored_day_snapshot(
        bank, "AAA", "2026-05-05")
    direct_row = probe.classify_snapshot_live(
        standalone_snapshot, expected_live_result(standalone_snapshot))
    check("live run: core fields are byte-equivalent, with durable provenance added",
          probe._canonical_json({key: value for key, value in clean_row.items()
                                 if key != "fetch_provenance"})
          == probe._canonical_json(direct_row))
    check("live run: standalone result binds a sealed source ledger",
          clean["fetch_ledger"]["verified"]
          and clean_row["fetch_provenance"]["source_companion_path"]
          == clean["fetch_ledger"]["report_path"]
          and clean_row["fetch_provenance"]["accepted"]["count"] == len(exact["stored"]))
    check("live run: exactly one request is recorded for one day",
          clean["request_count"] == 1
          and clean["max_requests_per_day"] == 1
          and clean_fetcher.calls == [("AAA", "2026-05-05")]
          and clean_fetcher.start_calls == clean_fetcher.close_calls == 1)
    check("live run: complete bank metadata equality is embedded",
          clean["bank_tree_unchanged"] is True
          and clean["bank_tree_before"] == clean["bank_tree_after"]
          and clean["bank_tree_before"]["entries"] == len(full_before))
    clean_artifact = json.loads(Path(clean["artifact"]).read_text("utf-8"))
    check("live run: artifact is report-only and strips minute payloads",
          clean_artifact["artifact_written"] is True
          and clean_artifact["bank_written"] is False
          and clean_artifact["report_only"] is True
          and "stored" not in clean_artifact["probes"][0]
          and "closes" not in clean_artifact["probes"][0])

    crwd_fetcher = FakeLiveFetcher()
    crwd = probe.run_live_probe(
        bank, ticker="CRWD", day="2026-06-26", owner="crwd",
        now=FIXED_NOW, live_fetcher=crwd_fetcher, gate_path=gate,
        run_logs_root=run_logs)
    check("live known answer: CRWD ledger factor produces MATCH",
          crwd["status"] == "complete"
          and crwd["probes"][0]["verdict"] == "MATCH"
          and crwd["probes"][0]["ledger_factor"] == 0.25,
          str(crwd["probes"][0]))

    ftnt_fetcher = FakeLiveFetcher()
    ftnt = probe.run_live_probe(
        bank, ticker="FTNT", day="2013-12-20", owner="ftnt",
        now=FIXED_NOW, live_fetcher=ftnt_fetcher, gate_path=gate,
        run_logs_root=run_logs)
    ftnt_row = ftnt["probes"][0]
    check("live known answer: FTNT deep correction stays documented",
          ftnt["status"] == "complete"
          and ftnt_row["verdict"] == "DOCUMENTED_DIVERGENCE"
          and ftnt_row["safety_status"] == "PASS"
          and ftnt_row["remedy"] is None,
          str(ftnt_row))

    regression_fetcher = FakeLiveFetcher(
        lambda snapshot: {
            "closes": dict(snapshot["stored"]),
            "volume": snapshot["stored_volume"],
        })
    regression = probe.run_live_probe(
        bank, ticker="FTNT", day="2013-12-20", owner="ftnt-regression",
        now=FIXED_NOW, live_fetcher=regression_fetcher, gate_path=gate,
        run_logs_root=run_logs)
    regression_row = regression["probes"][0]
    check("live correction guard: mismatch escalates without refetch remedy",
          regression["status"] == "review_required"
          and regression_row["safety_status"] == "REGRESSION"
          and regression_row["remedy"] == "correction_regression_review"
          and "refetch" not in regression_row["remedy"],
          str(regression_row))

    absent_fetcher = FakeLiveFetcher()
    absent = probe.run_live_probe(
        bank, ticker="AAA", day="2026-05-06", owner="absent",
        now=FIXED_NOW, live_fetcher=absent_fetcher, gate_path=gate,
        run_logs_root=run_logs)
    absent_row = absent["probes"][0]
    check("live known answer: verified NO_LIVE_DATA is pass-equivalent",
          absent["status"] == "complete"
          and absent_row["verdict"] == "NO_LIVE_DATA"
          and absent_row["snapshot_mode"] == "verified_absence"
          and absent_row["safety_status"] == "EXPECTED_SOURCE_ABSENCE"
          and absent_row["pass_equivalent"] is True,
          str(absent_row))

    reappeared_fetcher = FakeLiveFetcher(lambda _snapshot: {
        "closes": {"09:30:00": 10.0},
        "volume": 100,
        "raw_bar_count": 1,
        "accepted_bar_count": 1,
    })
    reappeared = probe.run_live_probe(
        bank, ticker="AAA", day="2026-05-06", owner="absence-return",
        now=FIXED_NOW, live_fetcher=reappeared_fetcher, gate_path=gate,
        run_logs_root=run_logs)
    reappeared_row = reappeared["probes"][0]
    check("live absence guard: reappearing source data needs review, not refetch",
          reappeared["status"] == "review_required"
          and reappeared_row["safety_status"] == "SOURCE_DATA_REAPPEARED"
          and reappeared_row["remedy"] == "source_absence_review"
          and "refetch" not in reappeared_row["remedy"],
          str(reappeared_row))

    random_fetcher = FakeLiveFetcher()
    random_run = probe.run_live_probe(
        bank, seed=42, count=5, owner="random", now=FIXED_NOW,
        live_fetcher=random_fetcher, gate_path=gate,
        run_logs_root=run_logs)
    check("live random: seed controls end-to-end bounded selection",
          random_run["seed"] == 42
          and random_run["selected_count"] == 5
          and random_run["request_count"] == 5
          and len(random_fetcher.calls) == len(set(random_fetcher.calls)) == 5
          and random_run["max_requests_per_day"] == 1,
          str(random_fetcher.calls))

    failed_fetcher = FakeLiveFetcher(fetch_error=RuntimeError("injected fetch"))
    failed = probe.run_live_probe(
        bank, ticker="BBB", day="2026-04-01", owner="fetch-error",
        now=FIXED_NOW, live_fetcher=failed_fetcher, gate_path=gate,
        run_logs_root=run_logs)
    failed_row = failed["probes"][0]
    check("live failure: one fetch error becomes one bounded PROBE_ERROR row",
          failed["status"] == "partial"
          and failed["probe_error_count"] == 1
          and failed["request_count"] == 1
          and failed_row["verdict"] == probe.PROBE_ERROR
          and failed_row["remedy"] == "rerun_live_probe"
          and "traceback" not in json.dumps(failed).lower(),
          str(failed_row))

    setup_fetcher = FakeLiveFetcher(start_error=RuntimeError("injected setup"))
    setup = probe.run_live_probe(
        bank, ticker="BBB", day="2026-04-01", owner="setup-error",
        now=FIXED_NOW, live_fetcher=setup_fetcher, gate_path=gate,
        run_logs_root=run_logs)
    check("live failure: connection setup failure sends zero historical requests",
          setup["status"] == "partial" and setup["request_count"] == 0
          and setup["probe_error_count"] == 1
          and not setup_fetcher.calls and setup_fetcher.close_calls == 1)

    race_bank = Path(tmp) / "race-bank"
    race_bank.mkdir()
    seed_series(race_bank, "RACE", 401, [dt.date(2026, 5, 5)])

    def mutate_manifest(snapshot):
        manifest = storage.load_manifest(race_bank / snapshot["ticker"])
        manifest["probe_race_fixture"] = True
        storage.save_manifest(race_bank / snapshot["ticker"], manifest)
        return expected_live_result(snapshot)

    race = probe.run_live_probe(
        race_bank, ticker="RACE", day="2026-05-05", owner="race",
        now=FIXED_NOW, live_fetcher=FakeLiveFetcher(mutate_manifest),
        gate_path=gate, run_logs_root=run_logs)
    check("live race: post-fetch manifest change fails closed and is fingerprinted",
          race["status"] == "partial" and race["probe_error_count"] == 1
          and race["bank_tree_unchanged"] is False
          and race["probes"][0]["evidence_current"] is False,
          str(race["probes"][0]))

    held_gate = Path(tmp) / "held-external-sweep.lock"
    held = operation_gate.acquire(
        "external_sweep", owner="selftest sweep", path=held_gate)
    blocked = FakeLiveFetcher()
    blocked_before = tree_snapshot(bank)
    try:
        expect_error(
            "live gate: held external_sweep excludes probe before setup",
            operation_gate.OperationBusy,
            lambda: probe.run_live_probe(
                bank, ticker="AAA", day="2026-05-05", owner="gate-busy",
                now=FIXED_NOW, live_fetcher=blocked, gate_path=held_gate,
                run_logs_root=run_logs))
    finally:
        held.release()
    check("live gate: exclusion makes no fetch and releases cleanly",
          not blocked.calls and blocked.start_calls == 0
          and tree_snapshot(bank) == blocked_before
          and operation_gate.status(path=held_gate)["available"])

    cli_fetcher = FakeLiveFetcher()
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        code = cli.main([
            "probe", "--allow-live", "--ticker", "AAA", "--day",
            "2026-05-05", "--owner", "cli"], root=bank, now=FIXED_NOW,
            live_fetcher=cli_fetcher, gate_path=gate,
            run_logs_root=run_logs)
    envelope = json.loads(out.getvalue())
    check("CLI: authorized explicit probe returns one versioned JSON envelope",
          code == 0 and envelope["kind"] == "live_spot_probe_report"
          and envelope["version"] == probe.REPORT_VERSION
          and envelope["status"] == "complete"
          and len(cli_fetcher.calls) == 1)

    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        code = cli.main(
            ["probe", "--allow-live", "--ticker", "AAA"], root=bank,
            now=FIXED_NOW, live_fetcher=FakeLiveFetcher(), gate_path=gate,
            run_logs_root=run_logs)
    error = json.loads(out.getvalue())
    check("CLI: incomplete explicit selection fails as bounded JSON",
          code == 2 and error["network"] is False
          and error["live_authorized"] is True
          and "traceback" not in out.getvalue().lower())

    physical = _physical_fetch(exact, minute_bars(dt.date(2026, 5, 5), base=10, count=1))
    adapter_result = physical.result
    check("IBKR adapter: one paced RTH TRADES request uses pinned conId",
          physical.error is None and len(physical.sends) == 1
          and physical.sends[0][1] == 101
          and tuple(physical.sends[0][2][key] for key in
                    ("durationStr", "barSizeSetting", "whatToShow")) == ("1 D", "1 min", "TRADES")
          and physical.sends[0][2]["useRTH"] is True
          and len(physical.turns) == 1 and physical.turns[0].kwargs["metered"] is False
          and physical.private_turns == []
          and adapter_result["accepted_bar_count"] == 1
          and physical.restored and physical.disconnected, repr(physical))

    check("live purity: complete fixture bank bytes and metadata are unchanged",
          tree_snapshot(bank) == full_before)
    check("live gate: normal run releases its operation lease",
          operation_gate.status(path=gate)["available"])


def embedded_path_semantics(bank, run_logs, tmp):
    bank = Path(bank)
    run_logs = Path(run_logs)
    bank_before = tree_snapshot(bank)
    artifacts_before = tree_snapshot(run_logs)

    selected_a = probe.build_embedded_plan(
        bank, ["AAA", "MISSING"], seed=51)
    selected_b = probe.build_embedded_plan(
        bank, ["MISSING", "AAA"], seed=51)
    check("embedded plan: one deterministic day per eligible ticker",
          selected_a["items"] == selected_b["items"]
          and set(selected_a["items"]) == {"AAA"}
          and selected_a["items"]["AAA"]["day"][:7] == "2026-05")
    check("embedded plan: ineligible ticker is one bounded error row",
          selected_a["selection_error_count"] == 1
          and selected_a["rows"][0]["ticker"] == "MISSING"
          and selected_a["rows"][0]["verdict"] == probe.PROBE_ERROR
          and selected_a["rows"][0]["request_count"] == 0)

    class Borrowed:
        port = 2000

        def __init__(self):
            self.disconnects = 0

        def disconnect(self):
            self.disconnects += 1

    borrowed = Borrowed()
    wrapper = probe.IBKRMinuteDayFetcher.from_borrowed_adapter(
        borrowed, ibkr_module=object(), pacer=object())
    wrapper.close()
    check("embedded fetch wrapper never closes the parent worker adapter",
          borrowed.disconnects == 0)

    original_acquire = probe.operation_gate.acquire
    original_writer = probe.write_artifact

    def forbidden(*_args, **_kwargs):
        raise AssertionError("embedded probe crossed a parent-owned boundary")

    fetcher = FakeLiveFetcher()
    try:
        probe.operation_gate.acquire = forbidden
        probe.write_artifact = forbidden
        # Operations owns the verified transport tripwires for real child
        # requests. Keep those exact guards installed; add only write traps.
        with deny_writes_network_processes(transport=False):
            row = probe.run_embedded_probe(
                bank, ticker="AAA", day="2026-05-05",
                live_fetcher=fetcher)
    finally:
        probe.operation_gate.acquire = original_acquire
        probe.write_artifact = original_writer
    check("embedded: caller-owned fetch produces one MATCH row",
          row["verdict"] == "MATCH"
          and row["evidence_source"] == "embedded_request"
          and row["request_count"] == 1
          and row["reused_request_count"] == 0
          and row["evidence_current"] is True,
          str(row))
    check("embedded: adapter lifecycle remains caller-owned",
          fetcher.calls == [("AAA", "2026-05-05")]
          and fetcher.start_calls == fetcher.close_calls == 0)
    check("embedded: no gate, artifact, bank, sidecar, or Run Logs write",
          tree_snapshot(bank) == bank_before
          and tree_snapshot(run_logs) == artifacts_before)
    check("embedded: public row strips stored and live minute payloads",
          "stored" not in row and "closes" not in row)

    snapshot = probe.stored_day_snapshot(bank, "AAA", "2026-05-05")
    reusable = _reusable_evidence(bank, snapshot)
    reused = probe.run_embedded_probe(
        bank, ticker="AAA", day="2026-05-05",
        live_evidence=reusable)
    check("embedded reuse: exact read-only post-boundary evidence is accepted",
          reused["verdict"] == "MATCH"
          and reused["evidence_source"] == "reused_read_only"
          and reused["request_count"] == 0
          and reused["reused_request_count"] == 1,
          str(reused))

    committed = dict(reusable, used_for_commit=True)
    rejected_commit = probe.run_embedded_probe(
        bank, ticker="AAA", day="2026-05-05",
        live_evidence=committed)
    check("embedded reuse: commit-derived payload is rejected as tautological",
          rejected_commit["verdict"] == probe.PROBE_ERROR
          and rejected_commit["request_count"] == 0
          and rejected_commit["reused_request_count"] == 0
          and "commit-derived" in rejected_commit["error"]["message"],
          str(rejected_commit))

    preboundary = dict(reusable, fetched_after_boundary=False)
    rejected_boundary = probe.run_embedded_probe(
        bank, ticker="AAA", day="2026-05-05",
        live_evidence=preboundary)
    check("embedded reuse: pre-repair-boundary evidence fails closed",
          rejected_boundary["verdict"] == probe.PROBE_ERROR
          and "repair boundary" in rejected_boundary["error"]["message"],
          str(rejected_boundary))

    wrong_day = dict(reusable, day="2026-05-04")
    rejected_identity = probe.run_embedded_probe(
        bank, ticker="AAA", day="2026-05-05",
        live_evidence=wrong_day)
    check("embedded reuse: ticker/day/manifest identity must match exactly",
          rejected_identity["verdict"] == probe.PROBE_ERROR
          and "stored boundary" in rejected_identity["error"]["message"],
          str(rejected_identity))

    failed = probe.run_embedded_probe(
        bank, ticker="BBB", day="2026-04-01",
        live_fetcher=FakeLiveFetcher(
            fetch_error=RuntimeError("injected embedded fetch")))
    check("embedded failure: one request becomes one bounded error row",
          failed["verdict"] == probe.PROBE_ERROR
          and failed["request_count"] == 1
          and failed["network"] is True
          and "traceback" not in json.dumps(failed).lower(),
          str(failed))

    ftnt = probe.run_embedded_probe(
        bank, ticker="FTNT", day="2013-12-20",
        live_fetcher=FakeLiveFetcher())
    check("embedded correction guard: FTNT stays documented divergence",
          ftnt["verdict"] == "DOCUMENTED_DIVERGENCE"
          and ftnt["safety_status"] == "PASS"
          and ftnt["remedy"] is None,
          str(ftnt))

    absent = probe.run_embedded_probe(
        bank, ticker="AAA", day="2026-05-06",
        live_fetcher=FakeLiveFetcher())
    check("embedded source absence: reviewed empty day remains pass-equivalent",
          absent["verdict"] == "NO_LIVE_DATA"
          and absent["safety_status"] == "EXPECTED_SOURCE_ABSENCE"
          and absent["pass_equivalent"] is True,
          str(absent))

    expect_error(
        "embedded contract: exactly one live evidence source is required",
        probe.LiveSpotProbeError,
        lambda: probe.run_embedded_probe(
            bank, ticker="AAA", day="2026-05-05"))
    expect_error(
        "embedded contract: fetch and reuse cannot both be supplied",
        probe.LiveSpotProbeError,
        lambda: probe.run_embedded_probe(
            bank, ticker="AAA", day="2026-05-05",
            live_fetcher=FakeLiveFetcher(), live_evidence=reusable))

    race_bank = Path(tmp) / "embedded-race-bank"
    race_bank.mkdir()
    seed_series(race_bank, "RACE2", 402, [dt.date(2026, 5, 5)])

    def mutate_manifest(snapshot_value):
        manifest = storage.load_manifest(
            race_bank / snapshot_value["ticker"])
        manifest["embedded_probe_race_fixture"] = True
        storage.save_manifest(race_bank / snapshot_value["ticker"], manifest)
        return expected_live_result(snapshot_value)

    race = probe.run_embedded_probe(
        race_bank, ticker="RACE2", day="2026-05-05",
        live_fetcher=FakeLiveFetcher(mutate_manifest))
    check("embedded race: post-fetch manifest mutation fails closed",
          race["verdict"] == probe.PROBE_ERROR
          and race["request_count"] == 1
          and race["evidence_current"] is False,
          str(race))


def daily_span_semantics(tmp):
    base = Path(tmp)
    bank = base / "daily-bank"
    run_logs = base / "daily-logs"
    gate = base / "daily-gate.lock"
    bank.mkdir()
    run_logs.mkdir()

    old_days = [dt.date(2025, 7, day) for day in (1, 2, 3)]
    new_days = [dt.date(2026, 2, day) for day in (2, 3, 4)]
    anchor = dt.date(2026, 6, 30)
    span_days = old_days + new_days + [anchor]
    action = {
        "date": "2026-01-15", "kind": "split", "factor": 0.25,
        "applies": "price", "source": "measured",
        "evidence": "daily fixture", "run": "selftest",
    }
    correction = {
        "type": "phantom_split_correction", "ticker": "FTD",
        "ex_date": "2026-01-15", "factor": 4,
        "intervals": ["1d"], "applied": "2026-07-21T00:00:00Z",
    }
    truncation = {
        "type": "identity_truncation", "ticker": "WBDD",
        "cutover": "2026-01-15", "intervals": ["1d"],
        "applied": "2026-07-21T00:00:00Z",
    }
    minute_only_note = {
        "type": "phantom_split_correction", "ticker": "SCOPE",
        "ex_date": "2026-01-15", "factor": 4,
        "intervals": ["1m"], "applied": "2026-07-21T00:00:00Z",
    }
    seed_daily_series(
        bank, "BASISD", 501, span_days, actions=[action])
    seed_daily_series(
        bank, "FTD", 502, span_days, corrections=[correction], base=200.0)
    seed_daily_series(
        bank, "WBDD", 503, span_days, corrections=[truncation], base=300.0)
    seed_daily_series(
        bank, "SMALL", 504, old_days[:2] + new_days + [anchor],
        actions=[action], base=400.0)
    seed_daily_series(
        bank, "SCOPE", 505, span_days,
        corrections=[minute_only_note], base=500.0)
    seed_series(bank, "MINUTE", 506, [dt.date(2026, 6, 30)])

    minute_default = probe.discover_candidates(bank)
    minute_explicit = probe.discover_candidates(bank, interval="1m")
    daily = probe.discover_candidates(bank, interval="1d")
    check("daily discovery: explicit 1d selects only daily-bearing tickers",
          [row["ticker"] for row in daily[0]]
          == ["BASISD", "FTD", "SCOPE", "SMALL", "WBDD"]
          and not daily[1] and daily[2] == 0,
          repr(daily))
    check("daily compatibility: omitted and explicit 1m discovery match",
          probe._canonical_json(minute_default)
          == probe._canonical_json(minute_explicit)
          and [row["ticker"] for row in minute_default[0]] == ["MINUTE"])
    kind_domains = [
        probe.discover_candidates(bank, interval=token)
        for token in ("1d-iv", "1d-hvol")
    ]
    check("daily sequencing: exact M2 kind tokens are recognized",
          all(result == ([], [], 0) for result in kind_domains),
          repr(kind_domains))
    expect_error(
        "daily sequencing: intraday kind tokens remain out of scope",
        probe.EvidenceError,
        lambda: probe.discover_candidates(bank, interval="1m-iv"))

    snapshot = probe.stored_daily_span_snapshot(
        bank, "BASISD", day=anchor)
    check("daily snapshot: rolling window is exactly 365 inclusive days",
          snapshot["span_start"] == "2025-07-01"
          and snapshot["span_end"] == "2026-06-30"
          and (dt.date.fromisoformat(snapshot["span_end"])
               - dt.date.fromisoformat(snapshot["span_start"])).days == 364
          and probe._daily_span_bounds("2024-02-29")
          == ("2023-03-02", "2024-02-29"),
          repr((snapshot["span_start"], snapshot["span_end"])))
    check("daily snapshot: every intersecting month is SHA-gated and digested",
          snapshot["stored_bars"] == 7
          and set(snapshot["month_sha256s"])
          == {"2025-07", "2026-02", "2026-06"}
          and len(snapshot["stored_digest"]) == 64
          and len(snapshot["span_fingerprint"]) == 64
          and snapshot["context_count"] == 2,
          repr(snapshot))
    correction_snapshot = probe.stored_daily_span_snapshot(
        bank, "FTD", day=anchor)
    check("daily context: action and correction boundaries are strictly prior",
          snapshot["contexts"][0]["context_end"] == "2026-01-14"
          and snapshot["contexts"][1]["context_start"] == "2026-01-15"
          and snapshot["contexts"][0]["basis_actions_applied"]
          and not snapshot["contexts"][1]["basis_actions_applied"]
          and correction_snapshot["contexts"][0]["context_end"]
          == "2026-01-14"
          and correction_snapshot["contexts"][0]["correction"]["mode"]
          == "expected_divergence"
          and correction_snapshot["contexts"][1]["context_start"]
          == "2026-01-15"
          and correction_snapshot["contexts"][1]["correction"] is None,
          repr((snapshot["contexts"], correction_snapshot["contexts"])))
    scoped = probe.stored_daily_span_snapshot(bank, "SCOPE", day=anchor)
    check("daily correction scope: a 1m-only note cannot affect 1d",
          all(segment["correction"] is None
              for segment in scoped["contexts"]), repr(scoped["contexts"]))

    def daily_payload(closes):
        return {
            "closes": closes,
            "volume": 0.0,
            "raw_bar_count": len(closes),
            "accepted_bar_count": len(closes),
            "discarded": {
                "invalid": 0, "outside_day": 0, "non_rth": 0},
        }

    scoped_stored = scoped["contexts"][0]["stored"]
    basis_step = probe.classify_daily_span_live(
        scoped, daily_payload({
            day: value * 2.0 for day, value in scoped_stored.items()}))
    scattered = probe.classify_daily_span_live(
        scoped, daily_payload({
            day: value * (2.0 + index)
            for index, (day, value) in enumerate(sorted(
                scoped_stored.items()))}))
    covered = dict(sorted(scoped_stored.items())[:2])
    coverage = probe.classify_daily_span_live(
        scoped, daily_payload(covered))
    no_live = probe.classify_daily_span_live(
        scoped, daily_payload({}))
    check("daily aggregate: one-context BASIS_STEP routing is preserved",
          basis_step["verdict"] == "BASIS_STEP"
          and basis_step["remedy"] == "basis_action_review"
          and basis_step["needs_human"] is True, repr(basis_step))
    check("daily aggregate: one-context SCATTERED routing is preserved",
          scattered["verdict"] == "SCATTERED_MISMATCH"
          and scattered["remedy"] == "month_refetch_candidate"
          and scattered["needs_human"] is True, repr(scattered))
    check("daily aggregate: one-context COVERAGE routing is preserved",
          coverage["verdict"] == "COVERAGE_MISMATCH"
          and coverage["remedy"] == "month_refetch_candidate"
          and coverage["needs_human"] is True, repr(coverage))
    check("daily aggregate: one-context NO_LIVE_DATA routing is preserved",
          no_live["verdict"] == "NO_LIVE_DATA"
          and no_live["safety_status"] == "COVERAGE_QUESTION"
          and no_live["remedy"] == "coverage_review", repr(no_live))

    daily_plan = probe.build_plan(
        bank, seed=7, count=3, now=FIXED_NOW, interval="1d")
    check("daily plan: span identity is public but stored payload is private",
          daily_plan["interval"] == "1d"
          and daily_plan["span_days"] == 365
          and daily_plan["request_duration"] == "1 Y"
          and all("stored" not in row and "contexts" not in row
                  and row["interval"] == "1d"
                  for row in daily_plan["probes"]), repr(daily_plan))
    default_minute_plan = probe.build_plan(
        bank, seed=9, count=1, now=FIXED_NOW)
    explicit_minute_plan = probe.build_plan(
        bank, seed=9, count=1, now=FIXED_NOW, interval="1m")
    check("daily compatibility: default and explicit 1m plans are identical",
          probe._canonical_json(default_minute_plan)
          == probe._canonical_json(explicit_minute_plan))

    daily_bank_before = tree_snapshot(bank)
    daily_logs_before = tree_snapshot(run_logs)
    original_acquire = probe.operation_gate.acquire
    original_writer = probe.write_artifact

    def daily_forbidden(*_args, **_kwargs):
        raise AssertionError("daily embedded probe crossed an owned boundary")

    basis_fetcher = FakeDailySpanFetcher()
    try:
        probe.operation_gate.acquire = daily_forbidden
        probe.write_artifact = daily_forbidden
        with deny_writes_network_processes(transport=False):
            basis_row = probe.run_embedded_probe(
                bank, ticker="BASISD", day=anchor,
                live_fetcher=basis_fetcher, interval="1d")
    finally:
        probe.operation_gate.acquire = original_acquire
        probe.write_artifact = original_writer
    check("daily embedded: one payload classifies both basis contexts",
          basis_row["verdict"] == "MATCH"
          and basis_row["segment_count"] == 2
          and basis_row["segment_verdict_counts"] == {"MATCH": 2}
          and basis_row["request_count"] == 1
          and basis_row["reused_request_count"] == 0
          and basis_row["span_request_count"] == 1
          and len(basis_fetcher.calls) == 1
          and all("request_count" not in row
                  and "reused_request_count" not in row
                  for row in basis_row["segments"]), repr(basis_row))
    check("daily embedded: basis context evidence stays explicit",
          basis_row["basis_actions"] == [{
              "date": "2026-01-15", "kind": "split", "factor": 0.25,
              "applies": "price",
          }]
          and basis_row["segments"][0]["basis_actions_applied"]["count"] == 1
          and len(basis_row["segments"][0]
                  ["basis_actions_applied"]["sha256"]) == 64
          and basis_row["segments"][1]
          ["basis_actions_applied"]["count"] == 0,
          repr(basis_row["segments"]))
    check("daily embedded: no gate, artifact, bank, or Run Logs write",
          tree_snapshot(bank) == daily_bank_before
          and tree_snapshot(run_logs) == daily_logs_before)

    mixed_closes = {}
    for context in snapshot["contexts"]:
        for index, (day, value) in enumerate(sorted(
                context["stored"].items())):
            factor = (0.5 if context["ledger_factor"] != 1.0
                      else 2.0 + index)
            mixed_closes[day] = value * factor
    mixed = probe.classify_daily_span_live(
        snapshot, daily_payload(mixed_closes))
    check("daily aggregate: mixed actionable remedy uses pinned severity",
          mixed["segment_verdict_counts"]
          == {"BASIS_STEP": 1, "SCATTERED_MISMATCH": 1}
          and mixed["verdict"] == "SCATTERED_MISMATCH"
          and mixed["remedy"] == "month_refetch_candidate", repr(mixed))

    ftnt_row = probe.run_embedded_probe(
        bank, ticker="FTD", day=anchor,
        live_fetcher=FakeDailySpanFetcher(), interval="1d")
    check("daily correction: pre-boundary divergence and later match share fetch",
          ftnt_row["verdict"] == "DOCUMENTED_DIVERGENCE"
          and ftnt_row["safety_status"] == "PASS"
          and ftnt_row["pass_equivalent"] is True
          and ftnt_row["segment_verdict_counts"]
          == {"DOCUMENTED_DIVERGENCE": 1, "MATCH": 1}, repr(ftnt_row))

    def correction_regression(value):
        result = expected_daily_live_result(value)
        for context in value["contexts"]:
            correction_value = context.get("correction")
            if (correction_value
                    and correction_value.get("mode")
                    == "expected_divergence"):
                result["closes"].update(context["stored"])
        result["raw_bar_count"] = len(result["closes"])
        result["accepted_bar_count"] = len(result["closes"])
        return result

    regression = probe.run_embedded_probe(
        bank, ticker="FTD", day=anchor,
        live_fetcher=FakeDailySpanFetcher(correction_regression),
        interval="1d")
    check("daily correction guard: restored old values are a loud regression",
          regression["safety_status"] == "REGRESSION"
          and regression["needs_human"] is True
          and regression["remedy"] == "correction_regression_review"
          and "refetch" not in regression["remedy"], repr(regression))

    wbd_row = probe.run_embedded_probe(
        bank, ticker="WBDD", day=anchor,
        live_fetcher=FakeDailySpanFetcher(), interval="1d")
    check("daily identity guard: stored pre-cutover WBD evidence is regression",
          wbd_row["safety_status"] == "REGRESSION"
          and wbd_row["needs_human"] is True
          and wbd_row["remedy"] == "correction_regression_review",
          repr(wbd_row))
    small = probe.run_embedded_probe(
        bank, ticker="SMALL", day=anchor,
        live_fetcher=FakeDailySpanFetcher(), interval="1d")
    check("daily fail-closed: undersized context never becomes MATCH",
          small["verdict"] == probe.PROBE_ERROR
          and small["safety_status"] == probe.PROBE_ERROR
          and any(segment["verdict"] == probe.PROBE_ERROR
                  for segment in small["segments"]), repr(small))

    reusable = _reusable_evidence(bank, snapshot)
    reused = probe.run_embedded_probe(
        bank, ticker="BASISD", day=anchor,
        live_evidence=reusable, interval="1d")
    check("daily reuse: exact post-boundary read-only span is accepted once",
          reused["verdict"] == "MATCH"
          and reused["request_count"] == 0
          and reused["reused_request_count"] == 1
          and reused["span_reuse_count"] == 1
          and reused["evidence_source"] == "reused_read_only", repr(reused))

    rejected = []
    for key in (
            "ticker", "interval", "anchor_day", "span_start", "span_end",
            "span_days", "span_fingerprint", "manifest_fingerprint",
            "stored_digest", "month_sha256s"):
        bad = dict(reusable)
        bad.pop(key)
        rejected.append(probe.run_embedded_probe(
            bank, ticker="BASISD", day=anchor,
            live_evidence=bad, interval="1d"))
    wrong_months = dict(reusable["month_sha256s"])
    wrong_months[sorted(wrong_months)[0]] = "f" * 64
    for key, value in (
            ("ticker", "OTHER"), ("interval", "1m"),
            ("anchor_day", "2026-06-29"),
            ("span_start", "2025-07-02"),
            ("span_end", "2026-06-29"), ("span_days", 364),
            ("span_fingerprint", "a" * 64),
            ("manifest_fingerprint", "b" * 64),
            ("stored_digest", "c" * 64),
            ("month_sha256s", wrong_months)):
        rejected.append(probe.run_embedded_probe(
            bank, ticker="BASISD", day=anchor,
            live_evidence=dict(reusable, **{key: value}), interval="1d"))
    for key, value in (
            ("read_only", False), ("used_for_commit", True),
            ("fetched_after_boundary", False)):
        rejected.append(probe.run_embedded_probe(
            bank, ticker="BASISD", day=anchor,
            live_evidence=dict(reusable, **{key: value}), interval="1d"))
    check("daily reuse: every identity/tautology mismatch fails without fallback",
          all(row["verdict"] == probe.PROBE_ERROR
              and row["request_count"] == 0
              and row["reused_request_count"] == 0
              for row in rejected), repr(rejected))
    expect_error(
        "daily embedded contract: request and reuse cannot both be supplied",
        probe.LiveSpotProbeError,
        lambda: probe.run_embedded_probe(
            bank, ticker="BASISD", day=anchor,
            live_fetcher=FakeDailySpanFetcher(), live_evidence=reusable,
            interval="1d"))

    outside = dt.date.fromisoformat(snapshot["span_start"]) - dt.timedelta(1)
    raw = daily_bars([dt.date.fromisoformat(day)
                      for day in sorted(snapshot["stored"])])
    raw += daily_bars([outside], base=999.0)
    physical = _physical_fetch(snapshot, raw)
    physical_result = physical.result
    call = physical.sends[0][2]
    check("daily adapter: exactly one non-metered RTH 1 Y TRADES request",
          physical.error is None and len(physical.sends) == 1 and physical.sends[0][1] == 501
          and tuple(call[key] for key in ("durationStr", "barSizeSetting", "whatToShow"))
          == ("1 Y", "1 day", "TRADES") and call["useRTH"] is True
          and call["endDateTime"].date().isoformat() == snapshot["span_end"]
          and call["endDateTime"].time() == dt.time(16, 0)
          and len(physical.turns) == 1 and physical.turns[0].kwargs["metered"] is False
          and physical.private_turns == [] and physical.parser_tokens == ["1d"]
          and physical_result["accepted_bar_count"] == snapshot["stored_bars"]
          and physical_result["fetch_provenance"]["dropped"] == 1
          and physical_result["raw_bar_count"] == snapshot["stored_bars"] + 1
          and physical.restored and physical.disconnected, repr(physical))

    bank_before = tree_snapshot(bank)
    standalone_fetcher = FakeDailySpanFetcher()
    report = probe.run_live_probe(
        bank, ticker="BASISD", day=anchor, owner="daily-span",
        now=FIXED_NOW, live_fetcher=standalone_fetcher,
        gate_path=gate, run_logs_root=run_logs, interval="1d")
    artifact = json.loads(Path(report["artifact"]).read_text("utf-8"))
    artifact_text = json.dumps(artifact, sort_keys=True)
    check("daily standalone: many days use one physical span request",
          report["status"] == "complete"
          and report["request_count"] == 1
          and report["span_request_count"] == 1
          and report["max_requests_per_span"] == 1
          and report["request_limit_per_span"] == 1
          and report["covered_day_count"] == snapshot["stored_bars"]
          and len(standalone_fetcher.calls) == 1, repr(report))
    check("daily artifact: report-only span strips all stored/live payloads",
          artifact["bank_written"] is False
          and artifact["report_only"] is True
          and '"stored"' not in artifact_text
          and '"closes"' not in artifact_text
          and tree_snapshot(bank) == bank_before)

    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        plan_code = cli.main([
            "plan", "--interval", "1d", "--seed", "1", "--count", "1"],
            root=bank, now=FIXED_NOW)
    plan_envelope = json.loads(out.getvalue())
    cli_fetcher = FakeDailySpanFetcher()
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        live_code = cli.main([
            "probe", "--allow-live", "--interval", "1d",
            "--ticker", "BASISD", "--day", anchor.isoformat(),
            "--owner", "daily-cli"], root=bank, now=FIXED_NOW,
            live_fetcher=cli_fetcher, gate_path=gate,
            run_logs_root=run_logs)
    live_envelope = json.loads(out.getvalue())
    check("daily CLI: explicit 1d plan and probe run end to end",
          plan_code == 0 and plan_envelope["interval"] == "1d"
          and live_code == 0 and live_envelope["interval"] == "1d"
          and live_envelope["request_count"] == 1
          and len(cli_fetcher.calls) == 1)
    blocked = FakeDailySpanFetcher()
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        blocked_code = cli.main([
            "probe", "--interval", "1d", "--ticker", "BASISD",
            "--day", anchor.isoformat()], root=bank, now=FIXED_NOW,
            live_fetcher=blocked, gate_path=gate, run_logs_root=run_logs)
    invalid_out = io.StringIO()
    with contextlib.redirect_stdout(invalid_out):
        invalid_code = cli.main(
            ["plan", "--interval", "1m-iv"], root=bank, now=FIXED_NOW)
    check("daily CLI safety: authorization and unsupported interval fail before fetch",
          blocked_code == 2 and not blocked.calls
          and invalid_code == 2
          and "traceback" not in invalid_out.getvalue().lower())


def daily_fail_closed_semantics(tmp):
    base = Path(tmp)
    days = ([dt.date(2025, 7, day) for day in (1, 2, 3)]
            + [dt.date(2026, 2, day) for day in (2, 3, 4)]
            + [dt.date(2026, 6, 30)])

    absence_root = base / "daily-absence"
    absence_root.mkdir()
    # Reappearance must be on a trading session, not Independence Day:
    # the request horizon deliberately filters closed-session provider rows.
    absent_day = dt.date(2025, 7, 7)
    seed_daily_series(
        absence_root, "ABSENT", 600, days,
        verified_absent=[absent_day.isoformat()])
    expected_absence = probe.run_embedded_probe(
        absence_root, ticker="ABSENT", day="2026-06-30", interval="1d",
        live_fetcher=FakeDailySpanFetcher())

    def reappear_absence(snapshot):
        result = expected_daily_live_result(snapshot)
        result["closes"][absent_day.isoformat()] = 777.0
        result["raw_bar_count"] = len(result["closes"])
        result["accepted_bar_count"] = len(result["closes"])
        return result

    reappeared = probe.run_embedded_probe(
        absence_root, ticker="ABSENT", day="2026-06-30", interval="1d",
        live_fetcher=FakeDailySpanFetcher(reappear_absence))
    check("daily absence: expected missing day remains pass-equivalent",
          expected_absence["verdict"] == "MATCH"
          and expected_absence["pass_equivalent"] is True
          and expected_absence["segments"][0]["verified_absent_count"] == 1,
          repr(expected_absence))
    check("daily absence: source reappearance is a loud coverage issue",
          reappeared["verdict"] == "COVERAGE_MISMATCH"
          and reappeared["safety_status"] == "SOURCE_DATA_REAPPEARED"
          and reappeared["pass_equivalent"] is False
          and reappeared["needs_human"] is True
          and reappeared["remedy"] == "source_absence_review"
          and reappeared["segments"][0]["source_reappeared_days"]
          == [absent_day.isoformat()], repr(reappeared))

    small_absent = dt.date(2026, 6, 26)
    seed_daily_series(
        absence_root, "ABSMALL", 599,
        [dt.date(2026, 6, 29), dt.date(2026, 6, 30)],
        verified_absent=[small_absent.isoformat()])

    def reappear_small(snapshot):
        result = expected_daily_live_result(snapshot)
        result["closes"][small_absent.isoformat()] = 778.0
        result["raw_bar_count"] = len(result["closes"])
        result["accepted_bar_count"] = len(result["closes"])
        return result

    small_reappeared = probe.run_embedded_probe(
        absence_root, ticker="ABSMALL", day="2026-06-30", interval="1d",
        live_fetcher=FakeDailySpanFetcher(reappear_small))
    check("daily absence: reappearance routing outranks a small context",
          small_reappeared["safety_status"] == "SOURCE_DATA_REAPPEARED"
          and small_reappeared["remedy"] == "source_absence_review"
          and small_reappeared["verdict"] == "COVERAGE_MISMATCH",
          repr(small_reappeared))

    small_truncation = {
        "type": "identity_truncation", "ticker": "IDSMALL",
        "cutover": "2026-07-01", "intervals": ["1d"],
        "applied": "2026-07-21T00:00:00Z",
    }
    seed_daily_series(
        absence_root, "IDSMALL", 598,
        [dt.date(2026, 6, 29), dt.date(2026, 6, 30)],
        corrections=[small_truncation])
    small_identity = probe.run_embedded_probe(
        absence_root, ticker="IDSMALL", day="2026-06-30", interval="1d",
        live_fetcher=FakeDailySpanFetcher())
    check("daily identity: categorical regression outranks sample size",
          small_identity["safety_status"] == "REGRESSION"
          and small_identity["remedy"] == "correction_regression_review",
          repr(small_identity))

    bounded_root = base / "daily-bounded-artifact"
    bounded_logs = base / "daily-bounded-logs"
    bounded_gate = base / "daily-bounded-gate.lock"
    bounded_root.mkdir()
    bounded_logs.mkdir()
    bounded_anchor = dt.date(2026, 6, 30)
    bounded_days = []
    bounded_cursor = bounded_anchor
    while len(bounded_days) < 256:
        if bounded_cursor.weekday() < 5:
            bounded_days.append(bounded_cursor)
        bounded_cursor -= dt.timedelta(days=1)
    bounded_days.sort()
    bounded_actions = [{
        "date": day.isoformat(), "kind": "split", "factor": 1.001,
        "applies": "price", "source": "measured",
        "evidence": "maximum action ledger", "run": "selftest",
    } for day in bounded_days]
    bounded_corrections = [{
        "type": "phantom_split_correction", "ticker": "BOUNDED",
        "ex_date": day.isoformat(), "factor": 1.001,
        "intervals": ["1d"], "applied": "2026-07-21T00:00:00Z",
    } for day in bounded_days]
    seed_daily_series(
        bounded_root, "BOUNDED", 597, bounded_days,
        actions=bounded_actions, corrections=bounded_corrections)
    bounded_report = probe.run_live_probe(
        bounded_root, ticker="BOUNDED", day=bounded_anchor,
        owner="bounded", now=FIXED_NOW,
        live_fetcher=FakeDailySpanFetcher(), gate_path=bounded_gate,
        run_logs_root=bounded_logs, interval="1d")
    bounded_artifact = Path(bounded_report["artifact"])
    bounded_payload = json.loads(bounded_artifact.read_text("utf-8"))
    bounded_row = bounded_payload["probes"][0]
    check("daily artifact: maximum ledgers are stored once and stay bounded",
          bounded_report["request_count"] == 1
          and bounded_artifact.stat().st_size < probe.MAX_ARTIFACT_BYTES
          and len(bounded_row["basis_actions"]) == probe.MAX_BASIS_ACTIONS
          and len(bounded_row["corrections"]) == probe.MAX_CORRECTIONS
          and all(isinstance(segment["basis_actions_applied"], dict)
                  and "records" not in (segment["correction"] or {})
                  for segment in bounded_row["segments"]),
          repr((bounded_artifact.stat().st_size,
                len(bounded_row["segments"]))))

    batch_payload = dict(bounded_payload)
    batch_payload["probes"] = []
    for index in range(probe.MAX_DAILY_COUNT):
        row = json.loads(json.dumps(bounded_row))
        row["ticker"] = f"BOUND{index}"
        batch_payload["probes"].append(row)
    batch_payload.update({
        "requested_count": probe.MAX_DAILY_COUNT,
        "selected_count": probe.MAX_DAILY_COUNT,
        "artifact": str(probe.artifact_path(
            "bounded-batch", run_logs_root=bounded_logs, now=FIXED_NOW)),
    })
    batch_artifact = Path(batch_payload["artifact"])
    probe.write_artifact(
        batch_artifact, batch_payload, bank_root=bounded_root,
        run_logs_root=bounded_logs)
    check("daily artifact: maximum default ten-row batch stays bounded",
          probe.MAX_DAILY_COUNT == probe.DEFAULT_COUNT == 10
          and len(batch_payload["probes"]) == 10
          and batch_artifact.stat().st_size < probe.MAX_ARTIFACT_BYTES,
          repr(batch_artifact.stat().st_size))
    expect_error(
        "daily batch cap: offline plan rejects eleven rows",
        probe.LiveSpotProbeError,
        lambda: probe.build_plan(
            bounded_root, count=11, now=FIXED_NOW, interval="1d"))

    limit_fetcher = FakeDailySpanFetcher()
    original_acquire = probe.operation_gate.acquire

    def no_daily_limit_gate(*_args, **_kwargs):
        raise AssertionError("daily count limit reached the operation gate")

    try:
        probe.operation_gate.acquire = no_daily_limit_gate
        for over_limit in (11, 100):
            expect_error(
                f"daily batch cap: count {over_limit} fails before the gate",
                probe.LiveSpotProbeError,
                lambda value=over_limit: probe.run_live_probe(
                    bounded_root, count=value, owner="bounded-limit",
                    now=FIXED_NOW, live_fetcher=limit_fetcher,
                    gate_path=bounded_gate, run_logs_root=bounded_logs,
                    interval="1d"))
    finally:
        probe.operation_gate.acquire = original_acquire
    check("daily batch cap: rejected counts make no live request",
          not limit_fetcher.calls)

    manifest_root = base / "daily-manifest-race"
    manifest_root.mkdir()
    seed_daily_series(manifest_root, "RACE", 601, days)

    def mutate_manifest(snapshot):
        manifest = storage.load_manifest(manifest_root / snapshot["ticker"])
        manifest["daily_race_fixture"] = True
        storage.save_manifest(manifest_root / snapshot["ticker"], manifest)
        return expected_daily_live_result(snapshot)

    manifest_race = probe.run_embedded_probe(
        manifest_root, ticker="RACE", day="2026-06-30", interval="1d",
        live_fetcher=FakeDailySpanFetcher(mutate_manifest))
    check("daily race: post-request manifest/action evidence fails closed",
          manifest_race["verdict"] == probe.PROBE_ERROR
          and manifest_race["request_count"] == 1
          and manifest_race["evidence_current"] is False,
          repr(manifest_race))

    file_root = base / "daily-file-race"
    file_root.mkdir()
    seed_daily_series(file_root, "FILE", 602, days)
    file_path = storage.month_file_path(
        file_root, "FILE", 2025, 7, "1d")

    def mutate_file(snapshot):
        raw = file_path.read_bytes()
        changed = raw.replace(b"100", b"101", 1)
        if changed == raw:
            raise AssertionError("daily file race fixture did not mutate")
        file_path.write_bytes(changed)
        return expected_daily_live_result(snapshot)

    file_race = probe.run_embedded_probe(
        file_root, ticker="FILE", day="2026-06-30", interval="1d",
        live_fetcher=FakeDailySpanFetcher(mutate_file))
    check("daily race: any consumed month mutation fails SHA revalidation",
          file_race["verdict"] == probe.PROBE_ERROR
          and file_race["request_count"] == 1
          and file_race["evidence_current"] is False,
          repr(file_race))

    bad_root = base / "daily-bad-evidence"
    bad_root.mkdir()
    seed_daily_series(bad_root, "BADSHA", 603, days)
    manifest = storage.load_manifest(bad_root / "BADSHA")
    manifest["intervals"]["1d"]["months"]["2025-07"]["sha256"] = "0" * 64
    storage.save_manifest(bad_root / "BADSHA", manifest)
    expect_error(
        "daily evidence: manifest SHA mismatch fails before payload use",
        probe.EvidenceError,
        lambda: probe.stored_daily_span_snapshot(
            bad_root, "BADSHA", day="2026-06-30"))

    seed_daily_series(bad_root, "BADROWS", 604, days)
    manifest = storage.load_manifest(bad_root / "BADROWS")
    entry = manifest["intervals"]["1d"]["months"]["2025-07"]
    entry["rows"] = int(entry["rows"]) + 1
    storage.save_manifest(bad_root / "BADROWS", manifest)
    expect_error(
        "daily evidence: manifest row-count drift fails closed",
        probe.EvidenceError,
        lambda: probe.stored_daily_span_snapshot(
            bad_root, "BADROWS", day="2026-06-30"))

    duplicate_dir = bad_root / "DUPDAY"
    duplicate_dir.mkdir()
    duplicate_manifest = storage.new_manifest("DUPDAY", "DUPDAY")
    duplicate_manifest["conid"] = 605
    duplicate_day = dt.date(2026, 6, 30)
    duplicate_path = storage.month_file_path(
        bad_root, "DUPDAY", 2026, 6, "1d")
    duplicate_manifest["intervals"] = {}
    duplicate_rows = daily_bars(
        [duplicate_day, duplicate_day], base=700.0)
    duplicate_stats = storage.write_month_file(
        duplicate_path, duplicate_rows[:1])
    # The public writer correctly refuses duplicates.  Corrupt the otherwise
    # valid fixture below that boundary so the probe's strict reader path is
    # what this case exercises, while keeping the manifest byte identity true.
    duplicate_payload = storage._bars_to_parquet(duplicate_rows)
    duplicate_path.write_bytes(duplicate_payload)
    duplicate_stats.update({
        "rows": len(duplicate_rows),
        "size": len(duplicate_payload),
        "sha256": hashlib.sha256(duplicate_payload).hexdigest(),
        "mtime_ns": duplicate_path.stat().st_mtime_ns,
    })
    storage.manifest_months(
        duplicate_manifest, "1d")["2026-06"] = duplicate_stats
    storage.save_manifest(duplicate_dir, duplicate_manifest)
    expect_error(
        "daily evidence: duplicate stored dates fail closed",
        probe.EvidenceError,
        lambda: probe.stored_daily_span_snapshot(
            bad_root, "DUPDAY", day=duplicate_day))

    orphan_root = base / "daily-orphan"
    orphan_root.mkdir()
    seed_daily_series(orphan_root, "ORPHAN", 606, days)
    manifest = storage.load_manifest(orphan_root / "ORPHAN")
    manifest["intervals"]["1d"]["months"].pop("2025-07")
    storage.save_manifest(orphan_root / "ORPHAN", manifest)
    expect_error(
        "daily evidence: unmanifested intersecting month fails closed",
        probe.EvidenceError,
        lambda: probe.stored_daily_span_snapshot(
            orphan_root, "ORPHAN", day="2026-06-30"))

    mixed_root = base / "daily-unrelated-corruption"
    mixed_root.mkdir()
    seed_series(mixed_root, "MIXED", 607, [dt.date(2026, 6, 30)])
    manifest = storage.load_manifest(mixed_root / "MIXED")
    manifest["intervals"]["1d"] = []
    storage.save_manifest(mixed_root / "MIXED", manifest)
    minute_candidates = probe.discover_candidates(mixed_root)
    daily_candidates = probe.discover_candidates(mixed_root, interval="1d")
    check("daily isolation: malformed 1d cannot poison default minute discovery",
          [row["ticker"] for row in minute_candidates[0]] == ["MIXED"]
          and not minute_candidates[1]
          and not daily_candidates[0]
          and daily_candidates[1][0]["ticker"] == "MIXED",
          repr((minute_candidates, daily_candidates)))

    failure_root = base / "daily-fetch-fail"
    failure_root.mkdir()
    seed_daily_series(failure_root, "FAIL", 608, days)
    failed_fetcher = FakeDailySpanFetcher(
        fetch_error=RuntimeError("injected daily failure"))
    failed = probe.run_embedded_probe(
        failure_root, ticker="FAIL", day="2026-06-30", interval="1d",
        live_fetcher=failed_fetcher)
    check("daily fetch failure: one attempt is bounded with no retry",
          failed["verdict"] == probe.PROBE_ERROR
          and failed["request_count"] == 1
          and failed["span_request_count"] == 1
          and len(failed_fetcher.calls) == 1
          and "traceback" not in json.dumps(failed).lower(), repr(failed))

    snapshot = probe.stored_daily_span_snapshot(
        failure_root, "FAIL", day="2026-06-30")

    def escaping_result(value):
        result = expected_daily_live_result(value)
        outside = (dt.date.fromisoformat(value["span_start"])
                   - dt.timedelta(days=1)).isoformat()
        result["closes"][outside] = 1.0
        result["raw_bar_count"] += 1
        result["accepted_bar_count"] += 1
        return result

    escaping = probe.run_embedded_probe(
        failure_root, ticker="FAIL", day="2026-06-30", interval="1d",
        live_fetcher=FakeDailySpanFetcher(escaping_result))
    escaping_error = ""
    try:
        probe.classify_daily_span_live(snapshot, escaping_result(snapshot))
    except probe.LiveFetchError as exc:
        escaping_error = str(exc)
    check("daily live identity: outside-span payload fails closed",
          "escapes" in escaping_error, escaping_error)
    check("daily raw transport: outside-span row is filtered before classification",
          escaping["verdict"] == "MATCH"
          and escaping["fetch_provenance"]["dropped"] == 1
          and escaping["raw_live_bars"] == escaping["accepted_live_bars"] + 1,
          repr(escaping))

    evidence = {key: snapshot[key] for key in (
        "ticker", "interval", "anchor_day", "span_start", "span_end",
        "span_days", "span_fingerprint", "manifest_fingerprint",
        "stored_digest", "month_sha256s")}
    evidence.update({
        "read_only": True, "used_for_commit": False,
        "fetched_after_boundary": True,
    })
    missing_live = probe.run_embedded_probe(
        failure_root, ticker="FAIL", day="2026-06-30", interval="1d",
        live_evidence=evidence)
    check("daily reuse: missing live result fails without a request",
          missing_live["verdict"] == probe.PROBE_ERROR
          and missing_live["request_count"] == 0
          and missing_live["reused_request_count"] == 0,
          repr(missing_live))

    setup_fetcher = FakeDailySpanFetcher(
        start_error=RuntimeError("injected daily setup"))
    setup_logs = base / "daily-setup-logs"
    setup_logs.mkdir()
    setup = probe.run_live_probe(
        failure_root, ticker="FAIL", day="2026-06-30", interval="1d",
        owner="daily-setup", now=FIXED_NOW, live_fetcher=setup_fetcher,
        gate_path=base / "daily-setup.lock", run_logs_root=setup_logs)
    check("daily setup failure: zero historical requests are recorded",
          setup["status"] == "partial" and setup["request_count"] == 0
          and setup["span_request_count"] == 0
          and not setup_fetcher.calls and setup_fetcher.close_calls == 1,
          repr(setup))

    physical = _physical_fetch(snapshot, [], error=RuntimeError("injected adapter failure"))
    check("daily adapter failure: RTH state and lifecycle are restored",
          isinstance(physical.error, ConnectionError)
          and str(physical.error) == "request failed: injected adapter failure"
          and physical.restored and physical.disconnected
          and len(physical.sends) == 1 and len(physical.turns) == 1
          and physical.turns[0].kwargs["metered"] is False and physical.private_turns == [],
          repr(physical))
    check("daily adapter failure: actual transport error is durably recorded",
          [event["payload"].get("error_type") for event in physical.events
           if event["event"] == "result"] == ["ConnectionError"])


class _FakePacer:
    def __init__(self):
        self.calls = []

    def wait_turn(self, cancel=None, metered=True):
        self.calls.append((cancel, metered))


def volatility_kind_semantics(tmp):
    base = Path(tmp)
    bank = base / "vol-kind-bank"
    run_logs = base / "vol-kind-logs"
    bank.mkdir()
    run_logs.mkdir()
    days = [dt.date(2026, 6, day) for day in (22, 23, 24, 25, 26)]
    anchor = days[-1]
    absent_day = dt.date(2026, 6, 18)  # a session, not the Juneteenth closure
    action = {
        "date": "2026-06-24", "kind": "split", "factor": 0.25,
        "applies": "price", "source": "measured",
        "evidence": "vol action isolation fixture", "run": "selftest",
    }
    listing_cut = {
        "type": "identity_listing_truncation", "cutover": "2020-01-01",
        "run": "row47-goog-shape-fixture",
    }
    seed_daily_series(
        bank, "IVVOL", 701, days, interval="1d-iv",
        values=[0.01] * len(days), actions=[action],
        corrections=[listing_cut],
        verified_absent=[absent_day.isoformat()])
    seed_daily_series(
        bank, "HVOL", 702, days, interval="1d-hvol",
        values=[0.0, 0.0, 0.01, 0.01, 0.01])
    seed_daily_series(
        bank, "BOTHVOL", 703, days, interval="1d-iv",
        values=[0.02] * len(days),
        verified_absent=[absent_day.isoformat()])

    def append_kind(ticker, interval, values, verified_absent=None):
        ticker_dir = bank / ticker
        manifest = storage.load_manifest(ticker_dir)
        grouped = {}
        for bar in daily_bars(
                days, interval=interval, values=values):
            grouped.setdefault(bar[0].strftime("%Y-%m"), []).append(bar)
        months = storage.manifest_months(manifest, interval)
        for month, bars in sorted(grouped.items()):
            year, number = int(month[:4]), int(month[5:7])
            path = storage.month_file_path(
                bank, ticker, year, number, interval)
            months[month] = storage.write_month_file(path, bars)
        if verified_absent is not None:
            manifest["intervals"][interval]["verified_absent"] = list(
                verified_absent)
        storage.save_manifest(ticker_dir, manifest)

    append_kind("BOTHVOL", "1d-hvol", [0.02] * len(days))

    iv_candidates = probe.discover_candidates(bank, interval="1d-iv")
    hvol_candidates = probe.discover_candidates(bank, interval="1d-hvol")
    check("vol discovery: IV/HVOL use exact manifest sections only",
          [row["ticker"] for row in iv_candidates[0]]
          == ["BOTHVOL", "IVVOL"]
          and [row["ticker"] for row in hvol_candidates[0]]
          == ["BOTHVOL", "HVOL"]
          and not iv_candidates[1] and not hvol_candidates[1]
          and iv_candidates[2] == hvol_candidates[2] == 0,
          repr((iv_candidates, hvol_candidates)))
    for token in ("1m-iv", "1m-hvol", "1d-bidask", "1d-pre"):
        expect_error(
            f"vol interval scope: unsupported token {token} is rejected",
            probe.EvidenceError,
            lambda token=token: probe.discover_candidates(
                bank, interval=token))

    keys = [day.isoformat() for day in days]
    stored = {key: 0.01 for key in keys}
    tolerance = probe.classify_day(
        stored, {key: 0.0101 for key in keys}, vol_mode=True)
    binary_boundary = probe.classify_day(
        {key: 0.0003 for key in keys},
        {key: 0.0004 for key in keys}, vol_mode=True)
    spread_boundary = probe.classify_day(
        stored, {key: stored[key] + delta for key, delta in zip(
            keys, (0.05, 0.05, 0.0501, 0.05, 0.05))},
        vol_mode=True)
    positive = probe.classify_day(
        stored, {key: 0.06 for key in keys}, vol_mode=True)
    negative = probe.classify_day(
        {key: 0.06 for key in keys},
        {key: 0.01 for key in keys}, vol_mode=True)
    scattered = probe.classify_day(
        stored, {key: stored[key] + delta for key, delta in zip(
            keys, (0.05, -0.005, 0.03, 0.0, 0.0))}, vol_mode=True)
    zero = probe.classify_day(
        {key: 0.0 for key in keys},
        {key: 0.0 for key in keys}, vol_mode=True)
    routed = probe.classify_probe(
        stored, {key: 0.06 for key in keys}, day=anchor,
        vol_mode=True)
    check("vol classifier: inclusive absolute tolerance and zero both MATCH",
          tolerance["verdict"] == "MATCH"
          and tolerance["off_bar_count"] == 0
          and binary_boundary["verdict"] == "MATCH"
          and binary_boundary["off_bar_count"] == 0
          and tolerance["vol_tolerance"] == probe.VOL_TOL == 0.0001
          and zero["verdict"] == "MATCH",
          repr((tolerance, binary_boundary, zero)))
    check("vol classifier: uniform signed offsets route to recompute review",
          positive["verdict"] == negative["verdict"]
          == probe.UNIFORM_RECOMPUTE
          and abs(positive["median_delta"] - 0.05) < 1e-12
          and abs(negative["median_delta"] + 0.05) < 1e-12
          and routed["remedy"] == "volatility_recompute_review"
          and routed["needs_human"] is True
          and routed["remedy"] not in {
              "basis_action_review", "month_refetch_candidate"},
          repr((positive, negative, routed)))
    check("vol classifier: inclusive delta-spread boundary stays uniform",
          spread_boundary["verdict"] == probe.UNIFORM_RECOMPUTE
          and abs(spread_boundary["max_delta_spread"]
                  - probe.VOL_SPREAD_TOL) < 1e-12,
          repr(spread_boundary))
    check("vol classifier: scattered additive drift stays a data problem",
          scattered["verdict"] == "SCATTERED_MISMATCH"
          and scattered["max_delta_spread"] > probe.VOL_SPREAD_TOL,
          repr(scattered))
    thin_live = probe.classify_day(
        {key: 0.01 for key in keys[:3]},
        {key: 0.51 for key in keys[:2]}, vol_mode=True)
    check("vol classifier: fewer than three shared observations cannot pass",
          thin_live["verdict"] == "COVERAGE_MISMATCH"
          and thin_live["shared"] == 2, repr(thin_live))
    sparse_majority = probe.classify_day(
        {key: 0.1 for key in keys[:3]},
        {keys[0]: 0.6, keys[1]: 0.6, keys[2]: 0.1},
        vol_mode=True)
    normal_isolated = probe.classify_day(
        {key: 0.1 for key in keys},
        {key: (0.6 if index < 2 else 0.1)
         for index, key in enumerate(keys)}, vol_mode=True)
    check("vol classifier: a sparse bad majority cannot use scatter amnesty",
          sparse_majority["verdict"] == "SCATTERED_MISMATCH"
          and sparse_majority["off_bar_count"] == 2
          and normal_isolated["verdict"] == "MATCH"
          and normal_isolated["off_bar_count"] == 2,
          repr((sparse_majority, normal_isolated)))
    expect_error(
        "vol classifier: price mode still rejects zero",
        probe.EvidenceError,
        lambda: probe.classify_day({keys[0]: 0.0}, {keys[0]: 0.0}))
    for label, bad in (("negative", -0.1), ("NaN", float("nan")),
                       ("infinite", float("inf"))):
        expect_error(
            f"vol classifier: {label} closes fail closed",
            probe.EvidenceError,
            lambda bad=bad: probe.classify_day(
                {keys[0]: bad}, {keys[0]: 0.0}, vol_mode=True))

    iv_snapshot = probe.stored_daily_span_snapshot(
        bank, "IVVOL", day=anchor, interval="1d-iv")
    hvol_snapshot = probe.stored_daily_span_snapshot(
        bank, "HVOL", day=anchor, interval="1d-hvol")
    check("vol snapshot: exact kind, zero, and split isolation survive storage",
          iv_snapshot["interval"] == "1d-iv"
          and hvol_snapshot["interval"] == "1d-hvol"
          and min(hvol_snapshot["stored"].values()) == 0.0
          and iv_snapshot["basis_actions"] == []
          and iv_snapshot["context_count"] == 1
          and all(context["ledger_factor"] == 1.0
                  and not context["basis_actions_applied"]
                  for context in iv_snapshot["contexts"])
          and iv_snapshot["corrections"][0]["type"]
          == "identity_listing_truncation",
          repr((iv_snapshot, hvol_snapshot)))
    check("vol snapshot: absence evidence remains scoped to its exact kind",
          absent_day.isoformat() in iv_snapshot["verified_absent"]
          and hvol_snapshot["verified_absent"] == [],
          repr((iv_snapshot["verified_absent"],
                hvol_snapshot["verified_absent"])))

    both_iv = probe.stored_daily_span_snapshot(
        bank, "BOTHVOL", day=anchor, interval="1d-iv")
    both_hvol = probe.stored_daily_span_snapshot(
        bank, "BOTHVOL", day=anchor, interval="1d-hvol")
    check("vol identity: same-ticker kind spans have distinct fingerprints",
          both_iv["manifest_fingerprint"] == both_hvol["manifest_fingerprint"]
          and both_iv["stored_digest"] == both_hvol["stored_digest"]
          and both_iv["span_fingerprint"] != both_hvol["span_fingerprint"]
          and absent_day.isoformat() in both_iv["verified_absent"]
          and both_hvol["verified_absent"] == [],
          repr((both_iv["span_fingerprint"],
                both_hvol["span_fingerprint"])))

    reuse_keys = (
        "ticker", "interval", "anchor_day", "span_start", "span_end",
        "span_days", "span_fingerprint", "manifest_fingerprint",
        "stored_digest", "month_sha256s")
    iv_evidence = _reusable_evidence(bank, both_iv)
    cross_kind = probe.run_embedded_probe(
        bank, ticker="BOTHVOL", day=anchor, interval="1d-hvol",
        live_evidence=iv_evidence)
    hvol_evidence = _reusable_evidence(bank, both_hvol)
    exact_reuse = probe.run_embedded_probe(
        bank, ticker="BOTHVOL", day=anchor, interval="1d-hvol",
        live_evidence=hvol_evidence)
    check("vol reuse: cross-kind evidence fails while exact-kind reuse passes",
          cross_kind["verdict"] == probe.PROBE_ERROR
          and cross_kind["request_count"] == 0
          and cross_kind["reused_request_count"] == 0
          and exact_reuse["verdict"] == "MATCH"
          and exact_reuse["reused_request_count"] == 1,
          repr((cross_kind, exact_reuse)))

    expected_absence = probe.run_embedded_probe(
        bank, ticker="IVVOL", day=anchor, interval="1d-iv",
        live_fetcher=FakeDailySpanFetcher())

    def reappear(snapshot):
        result = expected_daily_live_result(snapshot)
        result["closes"][absent_day.isoformat()] = 0.02
        result["raw_bar_count"] = len(result["closes"])
        result["accepted_bar_count"] = len(result["closes"])
        return result

    reappeared = probe.run_embedded_probe(
        bank, ticker="IVVOL", day=anchor, interval="1d-iv",
        live_fetcher=FakeDailySpanFetcher(reappear))
    check("vol kind gap: expected omission passes and reappearance is loud",
          expected_absence["verdict"] == "MATCH"
          and expected_absence["pass_equivalent"] is True
          and reappeared["verdict"] == "COVERAGE_MISMATCH"
          and reappeared["safety_status"] == "SOURCE_DATA_REAPPEARED"
          and reappeared["remedy"] == "source_absence_review"
          and reappeared["segments"][0]["source_reappeared_days"]
          == [absent_day.isoformat()], repr((expected_absence, reappeared)))

    absence_dir = bank / "ONLYABS"
    absence_dir.mkdir()
    absence_manifest = storage.new_manifest("ONLYABS", "ONLYABS")
    absence_manifest["conid"] = 704
    storage.manifest_months(absence_manifest, "1d-hvol")
    absence_manifest["intervals"]["1d-hvol"]["verified_absent"] = [
        anchor.isoformat()]
    storage.save_manifest(absence_dir, absence_manifest)
    absence_only = probe.run_embedded_probe(
        bank, ticker="ONLYABS", day=anchor, interval="1d-hvol",
        live_fetcher=FakeDailySpanFetcher())
    check("vol kind gap: an explicit verified-absence-only anchor is usable",
          absence_only["verdict"] == "NO_LIVE_DATA"
          and absence_only["safety_status"]
          == "EXPECTED_SOURCE_ABSENCE"
          and absence_only["pass_equivalent"] is True
          and absence_only["stored_bars"] == 0,
          repr(absence_only))

    bad_correction = {
        "type": "phantom_split_correction", "ticker": "BADVOL",
        "ex_date": "2026-06-24", "factor": 4,
        "intervals": ["1d-iv"],
        "applied": "2026-07-21T00:00:00Z",
    }
    bad_bank = base / "bad-vol-correction"
    bad_bank.mkdir()
    seed_daily_series(
        bad_bank, "BADVOL", 705, days, interval="1d-iv",
        values=[0.01] * len(days), corrections=[bad_correction])
    expect_error(
        "vol correction: a price-factor correction cannot target IV",
        probe.EvidenceError,
        lambda: probe.stored_daily_span_snapshot(
            bad_bank, "BADVOL", day=anchor, interval="1d-iv"))

    def payload(closes):
        return {
            "closes": closes, "volume": 0.0,
            "raw_bar_count": len(closes),
            "accepted_bar_count": len(closes),
            "discarded": {
                "invalid": 0, "outside_day": 0, "non_rth": 0},
        }

    expect_error(
        "vol live normalization: a negative ratio fails closed",
        probe.EvidenceError,
        lambda: probe.classify_daily_span_live(
            hvol_snapshot, payload({keys[0]: -0.1})))

    physical_rows = []
    for interval, snapshot, expected_what in (
            ("1d-iv", iv_snapshot, "OPTION_IMPLIED_VOLATILITY"),
            ("1d-hvol", hvol_snapshot, "HISTORICAL_VOLATILITY")):
        physical = _physical_fetch(snapshot,
            daily_bars([dt.date.fromisoformat(snapshot["anchor_day"])],
                       interval=interval, values=[0.0]))
        physical_rows.append((interval, expected_what, physical))
    check("vol adapter: each kind uses one exact non-metered RTH request",
          all(
              physical.error is None and physical.parser_tokens == [interval]
              and len(physical.sends) == 1
              and tuple(physical.sends[0][2][key] for key in
                        ("durationStr", "barSizeSetting", "whatToShow"))
              == ("1 Y", "1 day", expected_what)
              and physical.sends[0][2]["useRTH"] is True
              and len(physical.turns) == 1 and physical.turns[0].kwargs["metered"] is False
              and physical.private_turns == []
              and physical.result["closes"] == {anchor.isoformat(): 0.0}
              and physical.result["volume"] == 0.0
              and physical.restored and physical.disconnected
              for interval, expected_what, physical in physical_rows), repr(physical_rows))

    gate_calls = []
    original_acquire = probe.operation_gate.acquire

    def forbidden_gate(*_args, **_kwargs):
        gate_calls.append(True)
        raise AssertionError("daily cap must run before the operation gate")

    try:
        probe.operation_gate.acquire = forbidden_gate
        expect_error(
            "vol bound: daily kind count above ten fails before the gate",
            probe.LiveSpotProbeError,
            lambda: probe.run_live_probe(
                bank, count=11, interval="1d-iv", owner="vol-cap",
                now=FIXED_NOW, live_fetcher=FakeDailySpanFetcher(),
                gate_path=base / "vol-cap.lock", run_logs_root=run_logs))
    finally:
        probe.operation_gate.acquire = original_acquire
    check("vol bound: rejected count performs no gate or live work",
          not gate_calls)

    plan_results = []
    for interval in ("1d-iv", "1d-hvol"):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = cli.main([
                "plan", "--interval", interval, "--seed", "1",
                "--count", "1"], root=bank, now=FIXED_NOW)
        plan_results.append((interval, code, json.loads(out.getvalue())))
    check("vol CLI: both exact kind plans run end to end",
          all(code == 0 and envelope["interval"] == interval
              and envelope["selected_count"] == 1
              for interval, code, envelope in plan_results),
          repr(plan_results))

    blocked_fetcher = FakeDailySpanFetcher()
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        blocked_code = cli.main([
            "probe", "--interval", "1d-iv", "--ticker", "IVVOL",
            "--day", anchor.isoformat()], root=bank, now=FIXED_NOW,
            live_fetcher=blocked_fetcher,
            gate_path=base / "vol-blocked.lock", run_logs_root=run_logs)
    check("vol CLI: live authorization is checked before kind fetch",
          blocked_code == 2 and not blocked_fetcher.calls
          and not blocked_fetcher.start_calls)

    cli_runs = []
    for interval, ticker in (("1d-iv", "IVVOL"),
                             ("1d-hvol", "HVOL")):
        out = io.StringIO()
        fetcher = FakeDailySpanFetcher()
        with contextlib.redirect_stdout(out):
            code = cli.main([
                "probe", "--allow-live", "--interval", interval,
                "--ticker", ticker, "--day", anchor.isoformat(),
                "--owner", f"cli-{storage.kind_of(interval)}"],
                root=bank, now=FIXED_NOW, live_fetcher=fetcher,
                gate_path=base / f"cli-{storage.kind_of(interval)}.lock",
                run_logs_root=run_logs)
        cli_runs.append((
            interval, code, json.loads(out.getvalue()), fetcher))
    check("vol CLI: authorized IV and HVOL probes preserve exact identity",
          all(code == 0 and envelope["interval"] == interval
              and envelope["request_count"] == 1
              and len(fetcher.calls) == 1
              for interval, code, envelope, fetcher in cli_runs),
          repr([(interval, code, envelope.get("status"))
                for interval, code, envelope, _fetcher in cli_runs]))

    def uniform_result(snapshot):
        result = expected_daily_live_result(snapshot)
        result["closes"] = {
            key: value + 0.05
            for key, value in result["closes"].items()}
        result["raw_bar_count"] = len(result["closes"])
        result["accepted_bar_count"] = len(result["closes"])
        return result

    bank_before = tree_snapshot(bank)
    report = probe.run_live_probe(
        bank, ticker="IVVOL", day=anchor, interval="1d-iv",
        owner="vol-uniform", now=FIXED_NOW,
        live_fetcher=FakeDailySpanFetcher(uniform_result),
        gate_path=base / "vol-uniform.lock", run_logs_root=run_logs)
    check("vol report: recompute verdict and exact count domain are durable",
          report["interval"] == "1d-iv"
          and report["status"] == "review_required"
          and report["verdict_counts"][probe.UNIFORM_RECOMPUTE] == 1
          and set(report["verdict_counts"])
          == set(probe.VOL_VERDICTS) | {probe.PROBE_ERROR}
          and report["probes"][0]["remedy"]
          == "volatility_recompute_review"
          and tree_snapshot(bank) == bank_before,
          repr(report))


def fail_closed_fixtures(tmp):
    root = Path(tmp) / "bad-bank"
    root.mkdir()
    day = dt.date(2026, 6, 26)

    identity_vocab = {}
    for correction_type in (
            "identity_listing_truncation", "identity_truncation", "truncation"):
        note = {
            "type": correction_type,
            "ticker": "VOCAB",
            "cutover": "2026-01-15",
            "applied": "TEST",
        }
        if correction_type != "identity_listing_truncation":
            note["intervals"] = ["1d"]
        manifest = {"data_corrections": [note]}
        identity_vocab[correction_type] = {
            "exact": probe._validated_corrections(
                manifest, "VOCAB", "1d"),
            "base_family": probe._validated_corrections(
                manifest, "VOCAB", "1d-hvol"),
        }
    check("evidence: all identity types apply to exact and base-family scopes",
          all(
              len(rows["exact"]) == 1
              and rows["exact"][0]["mode"] == "expected_absent"
              and len(rows["base_family"]) == 1
              and rows["base_family"][0]["mode"] == "expected_absent"
              for rows in identity_vocab.values()),
          repr(identity_vocab))

    scoped_note = {
        "type": "truncation",
        "ticker": "VOCAB",
        "cutover": "2026-01-15",
        "intervals": ["1d"],
        "applied": "TEST",
    }
    check("evidence: nonmatching identity scope does not affect another family",
          probe._validated_corrections(
              {"data_corrections": [scoped_note]}, "VOCAB", "1m") == [])
    expect_error(
        "evidence: recognized identity record without required scope fails closed",
        probe.EvidenceError,
        lambda: probe._validated_corrections(
            {"data_corrections": [{
                "type": "identity_truncation",
                "ticker": "VOCAB",
                "cutover": "2026-01-15",
            }]},
            "VOCAB", "1d"))
    expect_error(
        "evidence: malformed listing cutover fails closed",
        probe.EvidenceError,
        lambda: probe._validated_corrections(
            {"data_corrections": [{
                "type": "identity_listing_truncation",
                "ticker": "VOCAB",
                "cutover": "not-a-date",
            }]},
            "VOCAB", "1d"))

    bad_action = {
        "date": "2026-06-29", "kind": "split", "factor": -1,
        "applies": "price", "source": "measured",
    }
    seed_series(root, "BADACT", 201, [day], actions=[bad_action])
    expect_error(
        "evidence: malformed basis action fails closed",
        probe.EvidenceError,
        lambda: probe.stored_month_snapshot(root, "BADACT", "2026-06"))

    first = {
        "date": "2026-06-29", "kind": "split", "factor": 0.5,
        "applies": "price", "source": "measured",
    }
    second = dict(first)
    second["kind"] = "scale"
    seed_series(root, "AMB", 202, [day], actions=[first, second])
    expect_error(
        "evidence: same-day price actions fail as ambiguous",
        probe.EvidenceError,
        lambda: probe.stored_month_snapshot(root, "AMB", "2026-06"))

    seed_series(root, "BADCOR", 203, [day], corrections=[])
    manifest = storage.load_manifest(root / "BADCOR")
    manifest["data_corrections"] = {}
    storage.save_manifest(root / "BADCOR", manifest)
    expect_error(
        "evidence: non-list correction record fails closed",
        probe.EvidenceError,
        lambda: probe.stored_month_snapshot(root, "BADCOR", "2026-06"))

    unknown = {
        "type": "mystery_repair", "ticker": "UNKNOWN",
        "intervals": ["1m"], "date": "2026-06-01",
    }
    seed_series(root, "UNKNOWN", 204, [day], corrections=[unknown])
    expect_error(
        "evidence: unknown minute correction type fails closed",
        probe.EvidenceError,
        lambda: probe.stored_month_snapshot(root, "UNKNOWN", "2026-06"))

    unscoped = {
        "type": "phantom_split_correction", "ticker": "UNSCOPED",
        "ex_date": "2026-06-29", "factor": 4,
    }
    seed_series(root, "UNSCOPED", 207, [day], corrections=[unscoped])
    expect_error(
        "evidence: unscoped correction record fails closed",
        probe.EvidenceError,
        lambda: probe.stored_month_snapshot(root, "UNSCOPED", "2026-06"))

    seed_series(root, "BADABS", 205, [day], verified_absent=[])
    manifest = storage.load_manifest(root / "BADABS")
    manifest["intervals"]["1m"]["verified_absent"] = "2026-06-25"
    storage.save_manifest(root / "BADABS", manifest)
    expect_error(
        "evidence: malformed verified_absent fails closed",
        probe.EvidenceError,
        lambda: probe.stored_month_snapshot(root, "BADABS", "2026-06"))

    seed_series(root, "BADSHA", 206, [day])
    manifest = storage.load_manifest(root / "BADSHA")
    manifest["intervals"]["1m"]["months"]["2026-06"]["sha256"] = "0" * 64
    storage.save_manifest(root / "BADSHA", manifest)
    expect_error(
        "evidence: manifest SHA mismatch fails before payload use",
        probe.EvidenceError,
        lambda: probe.stored_month_snapshot(root, "BADSHA", "2026-06"))

    seed_series(
        root, "BOTH", 208, [day], verified_absent=[day.isoformat()])
    expect_error(
        "evidence: a day cannot be both stored and verified absent",
        probe.EvidenceError,
        lambda: probe.stored_day_snapshot(root, "BOTH", day))

    seed_series(
        root, "ORPHAN", 209, [], verified_absent=[day.isoformat()])
    storage.write_month_file(
        storage.month_file_path(root, "ORPHAN", 2026, 6, "1m"),
        minute_bars(day))
    expect_error(
        "evidence: verified absence rejects an unmanifested month file",
        probe.EvidenceError,
        lambda: probe.stored_day_snapshot(root, "ORPHAN", day))

    candidates, errors, overflow = probe.discover_candidates(root)
    check("discovery: SHA candidate is retained for strict selected read",
          len(candidates) == 8 and not overflow
          and not any(row["ticker"] == "BADSHA" for row in errors),
          f"candidates={len(candidates)} errors={errors}")
    plan = probe.build_plan(root, seed=1, count=6, now=FIXED_NOW)
    check("plan: per-ticker evidence failures produce bounded error rows",
          plan["status"] == "partial" and plan["errors"]
          and all(len(row["reason"]) <= 300 for row in plan["errors"]),
          str(plan["errors"]))


def source_boundary():
    source = Path(probe.__file__).read_text(encoding="utf-8")
    cli_source = Path(cli.__file__).read_text(encoding="utf-8")
    display_source = (Path(probe.__file__).resolve().parent.parent
                      / "display_data.py").read_text(encoding="utf-8")
    display_tree = ast.parse(display_source)
    # C2 moved Fix Data and Add Stocks request dispatch into fixed engine functions.
    # Keep the same four automatic plan/request sites and minute-only oracle;
    # relocation must not silently remove the engine site from this fence.
    fixdata_source = Path(probe.__file__).with_name("fix_data_pipeline.py").read_text(encoding="utf-8")
    addstock_source = Path(probe.__file__).with_name("addstock_fetch_tasks.py").read_text(encoding="utf-8")
    embedded_calls = [
        node for tree in (display_tree, ast.parse(fixdata_source), ast.parse(addstock_source)) for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "live_spot_probe"
        and node.func.attr in {"build_embedded_plan", "run_embedded_probe"}
    ]
    check("surface: live adapter and operation gate are explicit boundaries",
          "class IBKRMinuteDayFetcher" in source
          and "def classify_snapshot_live" in source
          and "def run_embedded_probe" in source
          and "operation_gate.acquire" in source
          and 'importlib.import_module("stock_ibkr")' in source
          and "ib_async" not in source and "subprocess" not in source)
    command_action = next(
        action for action in cli._parser()._actions
        if action.dest == "command")
    check("surface: CLI exposes only reviewed plan/probe commands",
          set(command_action.choices) == {"plan", "probe"}
          and "--allow-live" in cli_source)
    check("surface: day-level adapter and classifier are explicit opt-in",
          "class IBKRDailySpanFetcher" in source
          and "def classify_daily_span_live" in source
          and getattr(probe, "DAYLEVEL_SPOT_CHECK", None) is True)
    check("surface: automatic Fix/Add callers remain explicitly minute-only",
          len(embedded_calls) == 4
          and all(any(
              keyword.arg == "interval"
              and isinstance(keyword.value, ast.Constant)
              and keyword.value.value == "1m"
              for keyword in call.keywords)
                  for call in embedded_calls)
          and "IBKRDailySpanFetcher" not in display_source)
    check("surface: live probe has no repair or bank-write call sites",
          all(token not in source for token in (
              "save_manifest(", "write_month_file(", "_commit_month(",
              "gap_fill(", "refetch_ticker(")))
    check("surface: price verdicts stay exact and vol adds only recompute",
          probe.VERDICTS == reference.VERDICTS
          and len(probe.VOL_VERDICTS) == len(reference.VERDICTS) + 1
          and set(probe.VOL_VERDICTS)
          == set(reference.VERDICTS) | {"UNIFORM_RECOMPUTE"})


def coordinator_semantics():
    calls = []

    def check_fn(adapter, pacer, ticker, rows, seed):
        calls.append(("check", ticker, adapter, pacer, len(rows), seed))
        return {"selection": {"day": "2026-06-25"}}

    def probe_fn(adapter, pacer, ticker, selection):
        calls.append(("probe", ticker, adapter, pacer, selection["day"]))
        return {
            "ticker": ticker, "day": selection["day"], "verdict": "MATCH",
            "pass_equivalent": True, "request_count": 1,
            "reused_request_count": 0, "network": True, "written": False,
        }

    coordinator = probe.AddStockProbeCoordinator(
        [("AAA", "1m"), ("AAA", "1d")], check_fn=check_fn,
        probe_fn=probe_fn, seed=17)
    coordinator.record_series([{"ticker": "AAA", "interval": "1m"}])
    check("coordinator: waits for every selected series",
          coordinator.claim() is None)
    coordinator.record_series([{"ticker": "AAA", "interval": "1d"}])
    ready_status = coordinator.status_snapshot()
    task = coordinator.claim()
    claimed_check_status = coordinator.status_snapshot()
    adapter, pacer = object(), object()
    coordinator.execute(task, adapter, pacer)
    ready_probe_status = coordinator.status_snapshot()
    probe_task = coordinator.claim()
    active_probe_status = coordinator.status_snapshot()
    coordinator.execute(probe_task, adapter, pacer)
    done_probe_status = coordinator.status_snapshot()
    result = coordinator.result()
    check("coordinator: check then one borrowed-adapter probe",
          task == ("check", "AAA")
          and probe_task[:2] == ("probe", "AAA")
          and calls[0][2:4] == (adapter, pacer)
          and calls[1][2:4] == (adapter, pacer)
          and result["requested_count"] == 1
          and result["completed_count"] == 1
          and result["request_count"] == 1
          and result["issue_count"] == 0,
          str((calls, result)))
    check("coordinator: observational status derives the probe k/N ledger",
          ready_status["check_ready"] == 1
          and ready_status["fill_waiting"] == 0
          and claimed_check_status["active_checks"] == 1
          and ready_probe_status["probe_ready"] == 1
          and ready_probe_status["probe_done"] == 0
          and active_probe_status["active_probes"] == 1
          and active_probe_status["probe_total"] == 1
          and done_probe_status["probe_done"] == 1
          and done_probe_status["probe_total"] == 1,
          str((ready_status, claimed_check_status, ready_probe_status,
               active_probe_status, done_probe_status)))

    def mixed_check(_adapter, _pacer, ticker, _rows, _seed):
        if ticker == "AAA":
            return {"row": {"ticker": ticker, "verdict": "NO_LIVE",
                            "pass_equivalent": True,
                            "request_count": 0,
                            "reused_request_count": 0}}
        return {"selection": {"day": "2026-07-21"}}

    mixed = probe.AddStockProbeCoordinator(
        [("AAA", "1m"), ("BBB", "1m")], check_fn=mixed_check,
        probe_fn=probe_fn, seed=19)
    mixed.record_series([
        {"ticker": "AAA", "interval": "1m"},
        {"ticker": "BBB", "interval": "1m"},
    ])
    mixed_start = mixed.status_snapshot()
    mixed.execute(mixed.claim(), adapter, pacer)
    mixed_terminal_check = mixed.status_snapshot()
    mixed.execute(mixed.claim(), adapter, pacer)
    mixed_probe_ready = mixed.status_snapshot()
    mixed_probe_task = mixed.claim()
    mixed_probe_active = mixed.status_snapshot()
    mixed.execute(mixed_probe_task, adapter, pacer)
    mixed_done = mixed.status_snapshot()
    check("coordinator: k/N is stable and counts every terminal ticker row",
          mixed_start["probe_done"] == 0
          and mixed_start["probe_total"] == 2
          and mixed_terminal_check["probe_done"] == 1
          and mixed_terminal_check["probe_total"] == 2
          and mixed_probe_ready["probe_done"] == 1
          and mixed_probe_ready["probe_total"] == 2
          and mixed_probe_active["probe_done"] == 1
          and mixed_probe_active["probe_total"] == 2
          and mixed_done["probe_done"] == 2
          and mixed_done["probe_total"] == 2,
          str((mixed_start, mixed_terminal_check, mixed_probe_ready,
               mixed_probe_active, mixed_done)))

    recovered = probe.AddStockProbeCoordinator(
        [("REC", "1m")], check_fn=check_fn, probe_fn=probe_fn, seed=18)
    recovered.record_series([
        {"ticker": "REC", "interval": "1m", "halt": "port lost"}])
    before = recovered.claim()
    recovered.record_series([{"ticker": "REC", "interval": "1m"}])
    check("coordinator: recovery success supersedes an earlier halt",
          before is None and recovered.claim() == ("check", "REC"))

    capped = probe.AddStockProbeCoordinator(
        [(ticker, "1m") for ticker in ("A", "B", "C")],
        check_fn=check_fn, probe_fn=probe_fn, seed=19)
    capped.record_series([
        {"ticker": ticker, "interval": "1m"}
        for ticker in ("A", "B", "C")])
    checks = [capped.claim() for _ in range(3)]
    capped.execute(checks[0], adapter, pacer)
    first_probe = capped.claim()
    capped.execute(checks[1], adapter, pacer)
    second_probe = capped.claim()
    blocked_third = capped.claim()
    capped.execute(checks[2], adapter, pacer)
    check("coordinator: probes cap at two while checks remain",
          all(task[0] == "check" for task in checks)
          and first_probe[0] == "probe" and second_probe[0] == "probe"
          and blocked_third is None)

    missing = probe.AddStockProbeCoordinator(
        [("MISS", "1m")], check_fn=check_fn, probe_fn=probe_fn, seed=20)
    missing_result = missing.result()
    check("coordinator: unprocessed ticker becomes a bounded error row",
          missing_result["completed_count"] == 1
          and missing_result["issue_count"] == 1
          and missing_result["rows"][0]["verdict"] == probe.PROBE_ERROR)

    fleet_down = probe.AddStockProbeCoordinator(
        [("DOWN1", "1m"), ("DOWN2", "1d")], check_fn=check_fn,
        probe_fn=probe_fn, seed=20)
    gate = probe.fleet_probe_gate(
        {"aborted_ports": {2000: "lost", 3000: "lost"}}, [2000, 3000])
    down_result = fleet_down.result(**gate)
    check("coordinator: an all-down fleet is one aggregate skip",
          gate == {"skip_reason": "fleet_down",
                   "down_ports": [2000, 3000]}
          and down_result["status"] == "skipped"
          and down_result["skipped_count"] == 2
          and down_result["completed_count"] == 0
          and down_result["issue_count"] == 0
          and down_result["rows"] == [])
    check("coordinator: a partially available fleet does not skip",
          probe.fleet_probe_gate(
              {"aborted_ports": {2000: "lost"}}, [2000, 3000]) == {})

    import threading
    cancelled_event = threading.Event()
    cancelled = probe.AddStockProbeCoordinator(
        [("STOP", "1m")], check_fn=check_fn, probe_fn=probe_fn, seed=21,
        cancel=cancelled_event)
    cancelled.record_series([{"ticker": "STOP", "interval": "1m"}])
    cancelled_event.set()
    cancelled_result = cancelled.result()
    check("coordinator: cancel starts no new work and reports the ticker",
          cancelled.claim() is None
          and cancelled_result["completed_count"] == 1
          and "cancelled" in cancelled_result["rows"][0]["error"]["message"])

    def broken_check(*_args):
        raise RuntimeError("synthetic accuracy failure")

    bounded = probe.AddStockProbeCoordinator(
        [("ERR", "1m")], check_fn=broken_check, probe_fn=probe_fn, seed=22)
    bounded.record_series([{"ticker": "ERR", "interval": "1m"}])
    bounded.execute(bounded.claim(), adapter, pacer)
    bounded_result = bounded.result()
    check("coordinator: accuracy failure is bounded and never probes",
          bounded_result["checked_count"] == 1
          and bounded_result["selected_count"] == 0
          and bounded_result["rows"][0]["verdict"] == probe.PROBE_ERROR
          and "synthetic accuracy failure"
          in bounded_result["rows"][0]["error"]["message"])


def _run_cases(*, temp_root):
    FAILS.clear()
    TOTAL[0] = 0
    reference_semantics()
    with tempfile.TemporaryDirectory(dir=temp_root) as tmp:
        fixture_semantics(tmp)
    with tempfile.TemporaryDirectory(dir=temp_root) as tmp:
        daily_span_semantics(tmp)
    with tempfile.TemporaryDirectory(dir=temp_root) as tmp:
        daily_fail_closed_semantics(tmp)
    with tempfile.TemporaryDirectory(dir=temp_root) as tmp:
        volatility_kind_semantics(tmp)
    with tempfile.TemporaryDirectory(dir=temp_root) as tmp:
        fail_closed_fixtures(tmp)
    coordinator_semantics()
    source_boundary()
    passed = TOTAL[0] - len(FAILS)
    print(f"\nLIVE SPOT PROBE SELF-TESTS: {passed}/{TOTAL[0]} passed")
    if FAILS:
        print("FAILED: " + ", ".join(FAILS))
        return 1
    return 0
