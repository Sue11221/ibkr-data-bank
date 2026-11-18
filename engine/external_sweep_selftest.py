"""Offline deterministic tests for WS7 external_sweep.py.

All reference responses are injected. Temporary fixture banks are the only
banks written. No external HTTP or TWS request is made.
"""

from __future__ import annotations

import ast
import contextlib
import datetime as dt
import hashlib
import io
import json
import subprocess
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

import external_sweep as sweep  # noqa: E402
import external_sweep_cli as cli  # noqa: E402
import external_sweep_reference as reference  # noqa: E402
import operation_gate  # noqa: E402
import stock_storage as storage  # noqa: E402


FAILS = []
TOTAL = [0]
FIXED_NOW = dt.datetime(2026, 7, 10, 18, 0, tzinfo=dt.timezone.utc)


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


def add_month(year, month, offset):
    number = (year * 12 + month - 1) + offset
    return number // 12, number % 12 + 1


def month_rows(year, month, count, ratio=1.0):
    return [
        (f"{y:04d}-{m:02d}", float(ratio), 3)
        for y, m in (add_month(year, month, index)
                     for index in range(count))
    ]


def seed_series(root, ticker, conid, ratios, provider_symbol=None,
                actions=None, start_year=2024, start_month=7):
    root = Path(root)
    ticker_dir = root / ticker
    ticker_dir.mkdir(parents=True, exist_ok=True)
    manifest = storage.new_manifest(ticker, ticker)
    manifest["conid"] = conid
    if provider_symbol is not None:
        manifest["provider_symbol"] = provider_symbol
    if actions is not None:
        manifest["actions"] = actions
    months = storage.manifest_months(manifest, "1d")
    external = {}
    for index, ratio in enumerate(ratios):
        year, month = add_month(start_year, start_month, index)
        bars = []
        first = dt.date(year, month, 1)
        while first.weekday() >= 5:
            first += dt.timedelta(days=1)
        for week in range(3):
            stamp = dt.datetime.combine(
                first + dt.timedelta(days=7 * week), dt.time())
            close = 100.0 * float(ratio)
            bars.append((stamp, close, close, close, close, 1000))
            external[stamp.date().isoformat()] = (
                100.0, 100.0, 100.0, 100.0, 1000)
        path = storage.month_file_path(root, ticker, year, month, "1d")
        months[f"{year:04d}-{month:02d}"] = storage.write_month_file(path, bars)
    storage.save_manifest(ticker_dir, manifest)
    return external


def tree_snapshot(root):
    root = Path(root)
    out = {}
    for path in [root] + sorted(root.rglob("*"), key=lambda item: str(item)):
        stat = path.stat()
        rel = "." if path == root else path.relative_to(root).as_posix()
        row = {
            "kind": "dir" if path.is_dir() else "file",
            "size": stat.st_size,
            "mtime_ns": stat.st_mtime_ns,
        }
        if path.is_file():
            row["sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
        out[rel] = row
    return out


class FakeClock:
    def __init__(self):
        self.value = 0.0
        self.sleeps = []

    def monotonic(self):
        return self.value

    def sleep(self, seconds):
        seconds = float(seconds)
        self.sleeps.append(seconds)
        self.value += seconds


class MapProvider:
    def __init__(self, references, *, clock=None, fail_count=0,
                 mutate=None, symbol_override=None, digest_override=None):
        self.references = dict(references)
        self.clock = clock
        self.fail_count = int(fail_count)
        self.mutate = mutate
        self.symbol_override = symbol_override
        self.digest_override = digest_override
        self.calls = []

    def fetch(self, symbol):
        self.calls.append({
            "symbol": symbol,
            "time": self.clock.monotonic() if self.clock is not None else None,
        })
        if self.mutate is not None:
            callback, self.mutate = self.mutate, None
            callback()
        if self.fail_count:
            self.fail_count -= 1
            raise OSError("injected provider failure")
        result = {
            "provider": sweep.PROVIDER,
            "provider_symbol": self.symbol_override or symbol,
            "range": sweep.REFERENCE_RANGE,
            "fetched_at": FIXED_NOW.isoformat(timespec="seconds"),
            "source_bytes_sha256": "a" * 64,
            "reference": self.references[symbol],
        }
        if self.digest_override is not None:
            result["response_digest"] = self.digest_override
        return result


def reference_semantics():
    for name, (months, expected) in reference.FIXTURES.items():
        result = sweep.classify(months)
        check(f"reference fixture {name} -> {expected}",
              result["verdict"] == expected, str(result))

    spinoff = sweep.classify(reference.FIXTURES["spinoff_offset"][0])
    check("reference spinoff factor remains about 0.8773",
          abs(spinoff["offset_factors"][0] - 0.8773) < 0.01,
          str(spinoff))
    check("reference 0.8773 is not split-like",
          not sweep.split_like(0.8773))
    check("reference phantom 0.25 is split-like",
          sweep.split_like(0.25))
    check("reference 2.0 / 0.5 / 1.5 are split-like",
          all(sweep.split_like(value) for value in (2.0, 0.5, 1.5)))
    check("reference 1.05 / 0.96 are not split-like",
          not any(sweep.split_like(value) for value in (1.05, 0.96)))
    recent_action = (month_rows(2025, 7, 12, 0.945901)
                     + [("2026-07", 1.0, 3)])
    recent_result = sweep.classify(recent_action)
    check("current anchor stays inside a newly detected action segment",
          recent_result["verdict"] == "HISTORIC_BASIS_OFFSET"
          and recent_result["current_ratio"] == 1.0
          and recent_result["current_segment_start"] == "2026-07"
          and recent_result["current_window_months"] == 1,
          str(recent_result))
    drift = sweep.classify(reference.FIXTURES["drifting_deep"][0])
    check("reference drift pins the old segment",
          drift.get("drift_segment", {}).get("start") == "2011-01")
    smooth = reference._months([
        (60, lambda _i, index: 0.9 - 0.4 * index / 59)])
    check("reference smooth venue drift has no step",
          not sweep.detect_steps(smooth))

    stored = {f"2020-{month:02d}-15": 10.0 for month in range(1, 13)}
    stored.update({
        f"2021-{month:02d}-15": 10.0 for month in range(1, 13)})
    external = dict(stored)
    check("reference injectable pipeline is CLEAN",
          sweep.classify(
              sweep.monthly_medians(stored, external))["verdict"] == "CLEAN")
    partial = {
        day: close for index, (day, close) in enumerate(sorted(stored.items()))
        if index % 2 == 0}
    monthly = sweep.monthly_medians(stored, partial)
    check("reference join uses shared dates only",
          len(monthly) == 12 and all(row[2] == 1 for row in monthly),
          str(monthly))
    short = sweep.classify(monthly[:8])
    check("reference insufficient overlap fails closed",
          short["verdict"] == "UNVERIFIABLE"
          and short["reason"] == "insufficient_overlap")

    stale = sweep.classify_with_recency(
        month_rows(2016, 3, 12), asof=FIXED_NOW)
    check("recency gate rejects CMCSA-shaped stale overlap",
          stale["verdict"] == "UNVERIFIABLE"
          and stale["reason"] == "overlap_not_current"
          and stale["latest_overlap_month"] == "2017-02"
          and stale["overlap_age_months"] == 113, str(stale))
    boundary = sweep.classify_with_recency(
        month_rows(2025, 5, 12), asof=FIXED_NOW)
    check("recency gate accepts overlap exactly three months old",
          boundary["verdict"] == "CLEAN"
          and boundary["latest_overlap_month"] == "2026-04"
          and boundary["overlap_age_months"] == 3, str(boundary))
    local_month_end = dt.datetime(
        2026, 7, 31, 23, 30,
        tzinfo=dt.timezone(dt.timedelta(hours=-4)))
    check("recency as-of preserves the injected local calendar month",
          sweep._asof_month(local_month_end) == "2026-07")

    action = {"date": "2026-01-01", "kind": "split", "factor": 0.25,
              "applies": "price", "source": "measured",
              "evidence": "fixture", "run": "selftest"}
    selected = sweep._validated_price_actions({"actions": [action]})
    check("basis helper emits bounded normalized price-action evidence",
          selected == [{"date": "2026-01-01", "kind": "split",
                        "factor": 0.25, "applies": "price"}], str(selected))
    expect_error(
        "basis helper rejects malformed action container",
        sweep.EvidenceError,
        lambda: sweep._validated_price_actions({"actions": action}))
    expect_error(
        "basis helper rejects ambiguous same-date price actions",
        sweep.EvidenceError,
        lambda: sweep._validated_price_actions({"actions": [
            action, dict(action, kind="price-basis", factor=0.5)]}))

    check("class-share provider mapping is exact and narrowly scoped",
          sweep._provider_identity_symbol("BF-B") == "BF.B"
          and sweep._provider_identity_symbol("BRK-B") == "BRK.B"
          and sweep._provider_identity_symbol("ABC-B") == "ABC-B")


def gate_tests(base):
    lock = Path(base) / "gate" / "operation.lock"
    first = operation_gate.acquire("fetch", path=lock)
    second = operation_gate.acquire("fetch", path=lock)
    check("gate same-mode acquisition is re-entrant",
          not first.released and not second.released)
    check("gate status is busy while fetch owns it",
          not operation_gate.status(path=lock)["available"])
    expect_error(
        "gate rejects conflicting mode in one process",
        operation_gate.OperationBusy,
        lambda: operation_gate.acquire("external_sweep", path=lock))
    second.release()
    first.release()
    check("gate becomes available after final nested release",
          operation_gate.status(path=lock)["available"])

    engine_dir = Path(__file__).resolve().parent
    child_lock = Path(base) / "gate" / "cross-process.lock"
    code = (
        "import sys,time; sys.path.insert(0,sys.argv[2]); "
        "import operation_gate as g; lease=g.acquire('fetch',path=sys.argv[1]); "
        "print('READY',flush=True); time.sleep(30)")
    child = subprocess.Popen(
        [sys.executable, "-c", code, str(child_lock), str(engine_dir)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        ready = child.stdout.readline().strip()
        check("gate child process acquired the lock", ready == "READY", ready)
        expect_error(
            "gate rejects a conflicting cross-process mode",
            operation_gate.OperationBusy,
            lambda: operation_gate.acquire(
                "external_sweep", path=child_lock))
    finally:
        child.terminate()
        child.wait(timeout=10)
    check("gate OS lock releases when owner process exits",
          operation_gate.status(path=child_lock)["available"])

    check("external_sweep import graph excludes stock_ibkr",
          "stock_ibkr" not in sys.modules)
    import stock_ibkr  # noqa: E402
    old_path = operation_gate.LOCK_PATH
    old_connect = stock_ibkr.LiveIB._connect_under_gate
    live_lock = Path(base) / "gate" / "liveib.lock"
    operation_gate.LOCK_PATH = live_lock
    stock_ibkr.LiveIB._connect_under_gate = lambda self: self
    adapter = stock_ibkr.LiveIB(ports=(2000,))
    try:
        adapter.connect()
        expect_error(
            "LiveIB connection owns fetch mode before any TWS call",
            operation_gate.OperationBusy,
            lambda: operation_gate.acquire(
                "external_sweep", path=live_lock))
    finally:
        adapter.disconnect()
        stock_ibkr.LiveIB._connect_under_gate = old_connect
        operation_gate.LOCK_PATH = old_path
    check("LiveIB disconnect releases the shared operation gate",
          operation_gate.status(path=live_lock)["available"])

    run_lock = Path(base) / "gate" / "logical-run.lock"
    old_path = operation_gate.LOCK_PATH
    operation_gate.LOCK_PATH = run_lock
    try:
        @stock_ibkr._fetch_run_gate
        def socketless_batch(*, fail=False):
            expect_error(
                "logical fetch run blocks sweep with no live socket",
                operation_gate.OperationBusy,
                lambda: operation_gate.acquire("external_sweep"))
            if fail:
                raise RuntimeError("injected batch failure")
            return "done"

        check("logical fetch run returns through guarded wrapper",
              socketless_batch() == "done")
        check("all public batch orchestrators hold the run-level gate",
              all(getattr(getattr(stock_ibkr, name),
                          "_holds_fetch_run_gate", False)
                  for name in ("gap_fill", "gap_fill_parallel",
                               "gap_fill_parallel_resilient")))
        expect_error(
            "logical fetch run propagates the batch exception",
            RuntimeError,
            lambda: socketless_batch(fail=True))
    finally:
        operation_gate.LOCK_PATH = old_path
    check("logical fetch run releases gate after return and exception",
          operation_gate.status(path=run_lock)["available"])


def provider_tests():
    data = []
    for month in range(1, 13):
        data.append({
            "t": f"2025-{month:02d}-15",
            "o": 10, "h": 11, "l": 9, "c": 10, "v": 1000,
        })
    raw = json.dumps({"data": data}).encode("utf-8")
    seen = []

    def opener(url, timeout):
        seen.append((url, timeout))
        return raw

    provider = sweep.StockAnalysisProvider(
        opener=opener, now=FIXED_NOW, timeout=7)
    from fetch_run_context import RequestRefused
    expect_error("A1 hold refuses the injected external opener", RequestRefused,
                 lambda: provider._read("https://stockanalysis.com/test"))
    check("A1 hold sends no external request", not seen)
    # Exercise the retained A2 parser only with this local opener fixture.
    with patch("fetch_ibkr_bridge.refuse_a2"):
        result = provider.fetch("TEST")
    parsed = __import__("urllib.parse", fromlist=["urlsplit"]).urlsplit(
        seen[0][0])
    check("provider uses only HTTPS stockanalysis.com",
          parsed.scheme == "https" and parsed.hostname == sweep.REFERENCE_HOST)
    check("provider sends the case-sensitive Max range",
          "range=Max" in seen[0][0] and "range=MAX" not in seen[0][0])
    check("provider bounds and parses the daily response",
          len(result["reference"]) == 12
          and sweep._valid_sha(result["source_bytes_sha256"])
          and result["response_digest"] == sweep.reference_digest(
              result["reference"]))

    old_url = sweep.validate.REF_URL
    sweep.validate.REF_URL = "https://example.com/api/{t}?range={r}"
    try:
        expect_error(
            "provider rejects a non-allowlisted host before opener",
            sweep.ProviderError,
            lambda: provider.fetch("TEST"))
    finally:
        sweep.validate.REF_URL = old_url


def fixture_tests(base):
    base = Path(base)
    project = base / "fixture-project"
    bank = project / storage.STORAGE_DIR_NAME
    run_logs = project / "Run Logs"
    cache = run_logs / "_external_sweep_cache"
    gate = run_logs / ".operation.lock"
    clean = [1.0 + (0.002 if index % 2 else -0.002)
             for index in range(24)]
    refs = {
        "AAA": seed_series(bank, "AAA", 101, clean),
        "BBB": seed_series(bank, "BBB", 202, clean),
    }
    before = tree_snapshot(bank)
    clock = FakeClock()
    provider = MapProvider(refs, clock=clock)
    writes = []
    original_atomic = storage._atomic_write_bytes
    original_urlopen = sweep.urllib.request.urlopen

    def guarded_atomic(path, raw):
        resolved = Path(path).resolve()
        bank_root = bank.resolve()
        if resolved == bank_root or bank_root in resolved.parents:
            raise AssertionError(f"attempted bank write: {resolved}")
        writes.append(resolved)
        return original_atomic(path, raw)

    def forbidden_network(*_args, **_kwargs):
        raise AssertionError("injected sweep attempted real network")

    storage._atomic_write_bytes = guarded_atomic
    sweep.urllib.request.urlopen = forbidden_network
    try:
        report = sweep._sweep_bank_body(
            bank, ["AAA", "BBB"], provider=provider,
            cache_root=cache, run_logs_root=run_logs, owner="selftest",
            gate_path=gate, pace=1.2, attempts=2, backoff=1.2,
            clock=clock.monotonic, sleep_fn=clock.sleep, now=FIXED_NOW)
    finally:
        storage._atomic_write_bytes = original_atomic
        sweep.urllib.request.urlopen = original_urlopen

    check("fixture sweep classifies both clean tickers",
          report["counts"]["CLEAN"] == 2
          and report["counts"]["UNVERIFIABLE"] == 0, str(report["counts"]))
    check("fixture sweep made exactly one request per cache miss",
          len(provider.calls) == 2 and report["network_requests"] == 2)
    check("fixture sweep enforces at least 1.2 seconds between requests",
          provider.calls[1]["time"] - provider.calls[0]["time"] >= 1.2,
          str(provider.calls))
    check("fixture writes are cache entries plus one artifact, all outside bank",
          len(writes) == 3
          and all(bank.resolve() not in path.parents for path in writes),
          str(writes))
    check("fixture complete bank bytes and metadata are unchanged",
          tree_snapshot(bank) == before)
    check("fixture artifact contains rows, counts, identities and evidence",
          Path(report["artifact"]).is_file()
          and all(row.get("conid") and row.get("reference_digest")
                  and row.get("manifest_fingerprint")
                  and isinstance(row.get("segments"), list)
                  and isinstance(row.get("steps"), list)
                  and isinstance(row.get("basis_actions_applied"), list)
                  and row.get("latest_overlap_month") == "2026-06"
                  for row in report["rows"]), str(report["rows"]))
    artifact_payload = json.loads(Path(report["artifact"]).read_text("utf-8"))
    check("fixture artifact is the versioned report-only WS7 schema",
          artifact_payload["kind"] == sweep.REPORT_KIND
          and artifact_payload["report_only"] is True
          and artifact_payload["params"]["ticker_count"] == 2)

    aaa_identity = sweep._manifest_record(bank, "AAA")
    expected = {
        key: aaa_identity[key]
        for key in ("ticker", "conid", "provider_symbol",
                    "manifest_fingerprint")}
    aaa_cache = sweep._cache_path(cache, expected)
    cache_before = aaa_cache.read_bytes()
    no_fetch = MapProvider(refs, fail_count=10)
    row = sweep._sweep_ticker_body(
        bank, "AAA", provider=no_fetch, cache_root=cache,
        attempts=2, sleep_fn=lambda _seconds: None, asof=FIXED_NOW)
    check("exact cache rerun performs zero fetches",
          row["verdict"] == "CLEAN"
          and row["reference_source"] == "cache"
          and row["network_requests"] == 0
          and not no_fetch.calls)
    check("exact cache rerun leaves cache bytes unchanged",
          aaa_cache.read_bytes() == cache_before)

    stale_ref = seed_series(
        bank, "CMCSA", 267748, clean[:12],
        start_year=2016, start_month=3)
    stale_provider = MapProvider({"CMCSA": stale_ref})
    stale_row = sweep._sweep_ticker_body(
        bank, "CMCSA", provider=stale_provider, cache_root=cache,
        attempts=1, sleep_fn=lambda _seconds: None, asof=FIXED_NOW)
    check("CMCSA-shaped stale ticker fails closed after a valid join",
          stale_row["verdict"] == "UNVERIFIABLE"
          and stale_row["reason"] == "overlap_not_current"
          and stale_row["latest_overlap_month"] == "2017-02"
          and stale_row["overlap_age_months"] == 113
          and len(stale_provider.calls) == 1, str(stale_row))

    crwd_action = {
        "date": "2026-01-01", "kind": "split", "factor": 0.25,
        "applies": "price", "source": "measured",
        "evidence": "fixture CRWD 4-for-1", "run": "selftest",
    }
    crwd_ref = seed_series(
        bank, "CRWD", 370757467, [4.0] * 18 + [1.0] * 6,
        actions=[crwd_action])
    crwd_before = tree_snapshot(bank / "CRWD")
    crwd_provider = MapProvider({"CRWD": crwd_ref})
    crwd_row = sweep._sweep_ticker_body(
        bank, "CRWD", provider=crwd_provider, cache_root=cache,
        attempts=1, sleep_fn=lambda _seconds: None, asof=FIXED_NOW)
    check("recorded CRWD-shaped price action normalizes history to CLEAN",
          crwd_row["verdict"] == "CLEAN"
          and crwd_row["stored_basis"] == "newest_recorded"
          and crwd_row["basis_actions_applied"] == [{
              "date": "2026-01-01", "kind": "split",
              "factor": 0.25, "applies": "price"}]
          and abs(crwd_row["current_ratio"] - 1.0) < 1e-12,
          str(crwd_row))
    check("basis-aware sweep leaves CRWD fixture bank bytes unchanged",
          tree_snapshot(bank / "CRWD") == crwd_before)

    spgi_action = dict(
        crwd_action, date="2026-07-01", kind="price-basis",
        factor=0.945901, evidence="fixture SPGI basis boundary")
    spgi_ref = seed_series(
        bank, "SPGI", 229629397, [1.0] * 24,
        actions=[spgi_action], start_year=2024, start_month=8)
    spgi_before = tree_snapshot(bank / "SPGI")
    spgi_provider = MapProvider({"SPGI": spgi_ref})
    spgi_row = sweep._sweep_ticker_body(
        bank, "SPGI", provider=spgi_provider, cache_root=cache,
        attempts=1, sleep_fn=lambda _seconds: None, asof=FIXED_NOW)
    check("recent SPGI-shaped action anchors to its one-month current segment",
          spgi_row["verdict"] == "HISTORIC_BASIS_OFFSET"
          and spgi_row["current_ratio"] == 1.0
          and spgi_row["current_segment_start"] == "2026-07"
          and spgi_row["current_window_months"] == 1
          and spgi_row["basis_actions_applied"][0]["factor"] == 0.945901,
          str(spgi_row))
    check("SPGI-shaped basis sweep leaves fixture bank bytes unchanged",
          tree_snapshot(bank / "SPGI") == spgi_before)

    bad_action_ref = seed_series(
        bank, "BADACT", 707, clean, actions=crwd_action)
    bad_action_provider = MapProvider({"BADACT": bad_action_ref})
    bad_action_row = sweep._sweep_ticker_body(
        bank, "BADACT", provider=bad_action_provider, cache_root=cache,
        attempts=1, sleep_fn=lambda _seconds: None, asof=FIXED_NOW)
    check("malformed manifest actions fail closed before network",
          bad_action_row["verdict"] == "UNVERIFIABLE"
          and "actions must be a list" in bad_action_row["reason"]
          and not bad_action_provider.calls, str(bad_action_row))

    class_refs = {
        "BF.B": seed_series(bank, "BF-B", 808, clean),
        "BRK.B": seed_series(bank, "BRK-B", 909, clean),
    }
    class_provider = MapProvider(class_refs)
    class_rows = [
        sweep._sweep_ticker_body(
            bank, ticker, provider=class_provider, cache_root=cache,
            attempts=1, sleep_fn=lambda _seconds: None, asof=FIXED_NOW)
        for ticker in ("BF-B", "BRK-B")
    ]
    check("class-share provider identities use exact dot symbols",
          [row["provider_symbol"] for row in class_rows] == ["BF.B", "BRK.B"]
          and [call["symbol"] for call in class_provider.calls]
          == ["BF.B", "BRK.B"]
          and all(row["verdict"] == "CLEAN" for row in class_rows),
          str(class_rows))
    class_no_fetch = MapProvider({}, fail_count=10)
    class_cached = sweep._sweep_ticker_body(
        bank, "BF-B", provider=class_no_fetch, cache_root=cache,
        attempts=1, sleep_fn=lambda _seconds: None, asof=FIXED_NOW)
    check("class-share cache identity retains mapped provider symbol",
          class_cached["verdict"] == "CLEAN"
          and class_cached["provider_symbol"] == "BF.B"
          and class_cached["reference_source"] == "cache"
          and not class_no_fetch.calls, str(class_cached))

    mismatched = dict(expected)
    mismatched["provider_symbol"] = "OTHER"
    check("cache identity mismatch is loud and unusable",
          sweep.load_cache(cache, mismatched)["status"] == "invalid")

    corrupt = json.loads(cache_before)
    corrupt["response_digest"] = "0" * 64
    aaa_cache.write_text(json.dumps(corrupt), encoding="utf-8")
    corrupt_before = aaa_cache.read_bytes()
    failing = MapProvider(refs, fail_count=10)
    retry_clock = FakeClock()
    row = sweep._sweep_ticker_body(
        bank, "AAA", provider=failing, cache_root=cache,
        pacer=sweep.RequestPacer(
            1.2, clock=retry_clock.monotonic, sleep_fn=retry_clock.sleep),
        attempts=2, backoff=1.2, sleep_fn=retry_clock.sleep,
        asof=FIXED_NOW)
    check("corrupt cache is never used when refresh fails",
          row["verdict"] == "UNVERIFIABLE"
          and row["cache_status"] == "invalid"
          and row["network_requests"] == 2, str(row))
    check("failed refresh preserves corrupt/last bytes for evidence",
          aaa_cache.read_bytes() == corrupt_before)
    aaa_cache.write_bytes(cache_before)

    retry_ref = seed_series(bank, "RETRY", 303, clean)
    retry_clock = FakeClock()
    retry_provider = MapProvider(
        {"RETRY": retry_ref}, clock=retry_clock, fail_count=1)
    retry_row = sweep._sweep_ticker_body(
        bank, "RETRY", provider=retry_provider, cache_root=cache,
        pacer=sweep.RequestPacer(
            1.2, clock=retry_clock.monotonic, sleep_fn=retry_clock.sleep),
        attempts=3, backoff=1.2, sleep_fn=retry_clock.sleep,
        asof=FIXED_NOW)
    check("bounded retry recovers and reports actual request count",
          retry_row["verdict"] == "CLEAN"
          and retry_row["network_requests"] == 2
          and len(retry_provider.calls) == 2, str(retry_row))
    check("retry starts remain paced",
          retry_provider.calls[1]["time"] - retry_provider.calls[0]["time"] >= 1.2,
          str(retry_provider.calls))

    race_ref = seed_series(bank, "RACE", 404, clean)
    race_manifest = bank / "RACE" / storage.MANIFEST_NAME

    def mutate_manifest():
        payload = json.loads(race_manifest.read_text("utf-8"))
        payload["injected_race"] = True
        race_manifest.write_text(json.dumps(payload), encoding="utf-8")

    racing = MapProvider({"RACE": race_ref}, mutate=mutate_manifest)
    race_row = sweep._sweep_ticker_body(
        bank, "RACE", provider=racing, cache_root=cache,
        attempts=1, sleep_fn=lambda _seconds: None, asof=FIXED_NOW)
    check("manifest race fails closed before cache commit",
          race_row["verdict"] == "UNVERIFIABLE"
          and "changed" in race_row["reason"], str(race_row))
    race_identity = sweep._manifest_record(bank, "RACE")
    check("manifest race leaves no cache for stale fingerprint",
          not sweep._cache_path(cache, race_identity).exists())

    mismatch_ref = seed_series(bank, "MISM", 505, clean)
    mismatch_provider = MapProvider(
        {"MISM": mismatch_ref}, symbol_override="OTHER")
    mismatch_row = sweep._sweep_ticker_body(
        bank, "MISM", provider=mismatch_provider, cache_root=cache,
        attempts=1, sleep_fn=lambda _seconds: None, asof=FIXED_NOW)
    check("provider identity mismatch fails closed",
          mismatch_row["verdict"] == "UNVERIFIABLE"
          and "provider symbol" in mismatch_row["reason"], str(mismatch_row))

    digest_ref = seed_series(bank, "DIG", 606, clean)
    digest_provider = MapProvider(
        {"DIG": digest_ref}, digest_override="0" * 64)
    digest_row = sweep._sweep_ticker_body(
        bank, "DIG", provider=digest_provider, cache_root=cache,
        attempts=1, sleep_fn=lambda _seconds: None, asof=FIXED_NOW)
    check("provider digest mismatch fails closed",
          digest_row["verdict"] == "UNVERIFIABLE"
          and "digest" in digest_row["reason"], str(digest_row))

    held_gate = run_logs / ".held.lock"
    held = operation_gate.acquire("fetch", path=held_gate)
    blocked_provider = MapProvider(refs)
    blocked_cache = run_logs / "blocked-cache"
    try:
        expect_error(
            "active fetch gate blocks sweep before provider call",
            operation_gate.OperationBusy,
            lambda: sweep._sweep_bank_body(
                bank, ["AAA"], provider=blocked_provider,
                cache_root=blocked_cache, run_logs_root=run_logs,
                owner="blocked", gate_path=held_gate, now=FIXED_NOW,
                sleep_fn=lambda _seconds: None))
    finally:
        held.release()
    check("blocked sweep made no provider call or cache directory",
          not blocked_provider.calls and not blocked_cache.exists())

    original_atomic = storage._atomic_write_bytes
    original_urlopen = sweep.urllib.request.urlopen
    storage._atomic_write_bytes = lambda *_a, **_k: (_ for _ in ()).throw(
        AssertionError("offline status attempted a write"))
    sweep.urllib.request.urlopen = lambda *_a, **_k: (_ for _ in ()).throw(
        AssertionError("offline status attempted network"))
    try:
        status = sweep.offline_status(bank, ["AAA"], cache_root=cache)
    finally:
        storage._atomic_write_bytes = original_atomic
        sweep.urllib.request.urlopen = original_urlopen
    check("offline status is write-free and network-free",
          status["network"] is False
          and status["rows"][0]["status"] == "ok")

    expect_error(
        "cache root inside bank is refused before any run",
        sweep.EvidenceError,
        lambda: sweep._sweep_bank_body(
            bank, ["AAA"], provider=MapProvider(refs),
            cache_root=bank / "cache", run_logs_root=run_logs,
            gate_path=gate, now=FIXED_NOW))
    expect_error(
        "artifact path inside bank is refused",
        sweep.EvidenceError,
        lambda: sweep.write_artifact(
            bank / "bad.json", {"x": 1}, bank_root=bank,
            run_logs_root=run_logs))
    expect_error(
        "artifact owner traversal is refused",
        sweep.ExternalSweepError,
        lambda: sweep.artifact_path("../bad", run_logs_root=run_logs))

    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        code = cli.main(
            [], root=bank, cache_root=cache, run_logs_root=run_logs,
            gate_path=gate, now=FIXED_NOW)
    payload = json.loads(out.getvalue())
    check("CLI default is offline status",
          code == 0 and payload["network"] is False
          and payload["kind"] == "external_sweep_offline_status")

    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        code = cli.main(
            ["sweep", "--ticker", "AAA"],
            root=bank, cache_root=cache, run_logs_root=run_logs,
            gate_path=gate, now=FIXED_NOW)
    payload = json.loads(out.getvalue())
    check("CLI refuses sweep without allow-network flag",
          code == 2 and payload["kind"] == "external_sweep_command_error")

    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        code = cli.main(
            ["sweep", "--allow-network"],
            root=bank, cache_root=cache, run_logs_root=run_logs,
            gate_path=gate, now=FIXED_NOW)
    payload = json.loads(out.getvalue())
    check("CLI requires --all for a whole-bank sweep",
          code == 2 and "--all" in payload["error"]["message"])

    cli_provider = MapProvider(refs, fail_count=10)
    cli_clock = FakeClock()
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        code = cli.main(
            ["sweep", "--allow-network", "--ticker", "AAA",
             "--owner", "cli-test"],
            provider=cli_provider, root=bank, cache_root=cache,
            run_logs_root=run_logs, gate_path=gate,
            clock=cli_clock.monotonic, sleep_fn=cli_clock.sleep,
            now=FIXED_NOW)
    payload = json.loads(out.getvalue())
    check("CLI explicit sweep remains held despite a legacy cache",
          code == 2 and payload["kind"] == "external_sweep_command_error"
          and payload["error"]["type"] == "AuthorityError"
          and not cli_provider.calls, str(payload))


def source_contract_tests():
    source = Path(sweep.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    imports = set()
    called_attributes = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imports.add(node.module or "")
        elif (isinstance(node, ast.Call)
              and isinstance(node.func, ast.Attribute)):
            called_attributes.add(node.func.attr)
    check("source contract has no stock_ibkr dependency",
          "stock_ibkr" not in imports)
    forbidden = {
        "save_manifest", "write_month_file", "record_basis_action",
        "apply_action",
        "record_validation", "apply_correction", "truncate",
    }
    check("source contract exposes no bank mutation call",
          not (called_attributes & forbidden),
          str(sorted(called_attributes & forbidden)))
    check("source contract names exactly six terminal verdicts",
          set(sweep.VERDICTS) == {
              "UNVERIFIABLE", "CURRENT_MISMATCH", "DRIFT_ANOMALY",
              "SEAM_CANDIDATE", "HISTORIC_BASIS_OFFSET", "CLEAN"})


def main():
    reference_semantics()
    provider_tests()
    source_contract_tests()
    with tempfile.TemporaryDirectory(prefix="external-sweep-selftest-") as tmp:
        gate_tests(tmp)
        fixture_tests(tmp)
    passed = TOTAL[0] - len(FAILS)
    print(f"\nEXTERNAL SWEEP SELF-TESTS: {passed}/{TOTAL[0]} passed")
    if FAILS:
        print("FAILED: " + ", ".join(FAILS))
        return 1
    print("ALL PASS (offline/injected; no live WS7 run)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
