"""Deterministic offline tests for the derived-daily cache.

All bank/cache writes are confined to temporary directories. No network,
production bank, operation gate, or run artifact is touched.
"""

from __future__ import annotations

import copy
import json
import os
import shutil
import sys
import tempfile
import threading
from datetime import date, datetime, time, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import derived_daily_cache as cache  # noqa: E402
import derived_daily_cache_bench as benchmark  # noqa: E402
import operation_gate  # noqa: E402
import stock_storage as storage  # noqa: E402
import stock_validate as validate  # noqa: E402


CHECKS = [0]
FAILS = []


def check(name, condition, detail=""):
    CHECKS[0] += 1
    status = "PASS" if condition else "FAIL"
    print(f"[{status}] {name}" + (f" -- {detail}" if detail and not condition else ""))
    if not condition:
        FAILS.append(name)


def expect_error(name, func, error=cache.DerivedDailyCacheError):
    try:
        func()
    except error:
        check(name, True)
    except Exception as exc:  # noqa: BLE001
        check(name, False, f"wrong error {type(exc).__name__}: {exc}")
    else:
        check(name, False, "no error")


def day_bars(day, count, base, *, start=time(9, 30), step_minutes=1):
    stamp = datetime.combine(day, start)
    bars = []
    for index in range(count):
        price = float(base + index * 0.25)
        bars.append((stamp, price, price + 0.2, price - 0.1,
                     price + 0.1, 100 + index))
        stamp += timedelta(minutes=step_minutes)
    return bars


def seed_series(root, ticker, interval, months):
    ticker = storage.canonical_ticker(ticker)
    ticker_dir = Path(root) / ticker
    ticker_dir.mkdir(parents=True, exist_ok=True)
    manifest = storage.load_manifest(ticker_dir) or storage.new_manifest(ticker, ticker)
    target = storage.manifest_months(manifest, interval)
    paths = {}
    for month, bars in sorted(months.items()):
        year, number = int(month[:4]), int(month[5:7])
        path = storage.month_file_path(
            root, ticker, year, number, interval, fmt="csv")
        stats = storage.write_month_file(path, bars)
        target[month] = {**stats, "status": "present", "source": "fixture"}
        paths[month] = path
    storage.save_manifest(ticker_dir, manifest)
    return paths


def legacy_daily(root, ticker="AAA", interval="1m", min_bars=0):
    return validate.derive_daily(
        validate.read_series(root, ticker, interval), min_bars=min_bars)


def read_document(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def write_document(path, document):
    Path(path).write_text(
        json.dumps(document, sort_keys=True, separators=(",", ":"),
                   allow_nan=False), encoding="utf-8")


def fixture_fingerprint(_root, ticker, interval):
    return {
        "schema_version": storage.INTERVAL_FINGERPRINT_VERSION,
        "algorithm": "sha256",
        "sha256": "a" * 64,
        "ticker": storage.canonical_ticker(ticker),
        "interval": interval,
        "present": True,
        "backfill_incomplete": False,
        "month_count": 1,
        "verified_absent_count": 0,
    }


def test_cold_warm_and_edges():
    base = Path(tempfile.mkdtemp(prefix="ddc_edges_"))
    try:
        bank = base / "bank"
        cache_root = base / "cache"
        jan2, jan3, jan4, feb1 = (
            date(2024, 1, 2), date(2024, 1, 3),
            date(2024, 1, 4), date(2024, 2, 1))
        months = {
            "2024-01": (day_bars(jan2, 4, 10.0)
                        + day_bars(jan3, 3, 20.0)
                        + day_bars(jan4, 1, 30.0)),
            "2024-02": day_bars(feb1, 2, 40.0),
        }
        seed_series(bank, "AAA", "1m", months)
        expected = legacy_daily(bank)
        cold, cold_stats = cache.derive_series(
            bank, "aaa", "1m", cache_root=cache_root)
        check("cold derivation matches legacy aggregator", cold == expected)
        check("cold run decodes every source month",
              cold_stats["cache_hits"] == 0
              and cold_stats["cache_misses"] == 2
              and cold_stats["source_months_decoded"] == 2
              and cold_stats["source_rows_decoded"] == sum(map(len, months.values())))
        check("cold source is current and cacheable",
              cold_stats["source_current"] is True
              and cold_stats["cacheable"] is True
              and cold_stats["persistence_attempted"] is True
              and cold_stats["persistence_succeeded"] is True)

        path = Path(cold_stats["cache_path"])
        before_cache = path.stat()
        original_reader = cache._read_source_month
        cache._read_source_month = lambda *_args: (_ for _ in ()).throw(
            AssertionError("warm cache decoded a month"))
        try:
            warm, warm_stats = cache.derive_series(
                bank, "AAA", "1m", cache_root=cache_root)
        finally:
            cache._read_source_month = original_reader
        after_cache = path.stat()
        check("warm run is bit-identical with zero source decode",
              warm == expected and warm_stats["cache_hits"] == 2
              and warm_stats["cache_misses"] == 0
              and warm_stats["source_rows_decoded"] == 0)
        check("warm hit does not touch cache bytes or mtime",
              before_cache.st_size == after_cache.st_size
              and before_cache.st_mtime_ns == after_cache.st_mtime_ns
              and warm_stats["persistence_attempted"] is False)

        filtered, filtered_stats = cache.derive_series(
            bank, "AAA", "1m", min_bars=3, cache_root=cache_root)
        check("cached counts support caller-specific min_bars",
              set(filtered) == {jan2, jan3}
              and filtered_stats["cache_hits"] == 2)

        scrambled = list(reversed(day_bars(date(2024, 1, 5), 3, 50.0)))
        scrambled.append((datetime(2024, 1, 5, 18, 0), 999.0, 999.0,
                          999.0, 999.0, 1))
        records = cache._aggregate_month(scrambled, "2024-01")
        expected_scrambled = validate.derive_daily(scrambled)
        actual_scrambled = cache._records_to_daily(
            {"2024-01": cache._cache_entry(
                cache._source_from_manifest("2024-01", {
                    "status": "present", "sha256": "a" * 64,
                    "rows": 3, "size": 1, "mtime_ns": 1,
                    "first": "1/5/2024 9:30:00",
                    "last": "1/5/2024 9:32:00",
                }), records)}, 0)
        check("out-of-order and off-hours aggregation matches legacy",
              actual_scrambled == expected_scrambled)

        check("injected cache path stays outside the bank",
              not cache._is_within(path.resolve(), bank.resolve())
              and cache._is_within(path.resolve(), cache_root.resolve()))
        check("default cache root is project-level and gitignored",
              cache.default_cache_root(bank) == base / "_derived_daily_cache"
              and "_derived_daily_cache/" in (
                  Path(__file__).resolve().parent.parent / ".gitignore"
              ).read_text(encoding="utf-8"))

        original_manifest = cache._read_manifest
        manifest_calls = []
        cache._read_manifest = lambda *_args: manifest_calls.append(1)
        try:
            for token in ("1m-iv", "1m-pre", "1m-bidask-pre", "1d"):
                expect_error(
                    f"unsupported token {token} fails before source/cache lookup",
                    lambda token=token: cache.derive_series(
                        bank, "AAA", token, cache_root=cache_root))
        finally:
            cache._read_manifest = original_manifest
        check("unsupported tokens made no manifest read", not manifest_calls)
        expect_error("cache root inside bank is rejected",
                     lambda: cache.derive_series(
                         bank, "AAA", "1m", cache_root=bank / "cache"))
        expect_error("malformed ticker cannot escape cache root",
                     lambda: cache.derive_series(
                         bank, "../..", "1m", cache_root=cache_root),
                     storage.StorageError)
    finally:
        shutil.rmtree(base, ignore_errors=True)


def test_invalidation_and_bounds():
    base = Path(tempfile.mkdtemp(prefix="ddc_invalid_"))
    try:
        bank, cache_root = base / "bank", base / "cache"
        jan = day_bars(date(2024, 1, 8), 3, 10.0)
        feb = day_bars(date(2024, 2, 8), 3, 20.0)
        seed_series(bank, "AAA", "1m", {"2024-01": jan, "2024-02": feb})
        first, stats = cache.derive_series(
            bank, "AAA", "1m", cache_root=cache_root)
        path = Path(stats["cache_path"])

        changed = day_bars(date(2024, 1, 8), 4, 30.0)
        seed_series(bank, "AAA", "1m", {"2024-01": changed})
        updated, changed_stats = cache.derive_series(
            bank, "AAA", "1m", cache_root=cache_root)
        check("one changed month misses while unchanged month hits",
              changed_stats["cache_hits"] == 1
              and changed_stats["cache_misses"] == 1
              and updated == legacy_daily(bank) and updated != first)

        all_manifest_gates = True
        replacements = {
            "sha256": "f" * 64,
            "rows": 999,
            "size": 999,
            "mtime_ns": 999,
            "first": "1/1/2024 9:30:00",
            "last": "1/31/2024 15:59:00",
        }
        for field, value in replacements.items():
            document = read_document(path)
            document["months"]["2024-01"]["source"][field] = value
            write_document(path, document)
            result, gate_stats = cache.derive_series(
                bank, "AAA", "1m", cache_root=cache_root)
            all_manifest_gates &= (
                result == legacy_daily(bank)
                and gate_stats["cache_hits"] == 1
                and gate_stats["cache_misses"] == 1)
        check("manifest SHA/rows/size/mtime/first/last mismatches read through",
              all_manifest_gates)

        document = read_document(path)
        document["months"]["2024-01"]["records_sha256"] = "0" * 64
        write_document(path, document)
        result, digest_stats = cache.derive_series(
            bank, "AAA", "1m", cache_root=cache_root)
        check("daily digest mismatch invalidates the whole document",
              result == legacy_daily(bank)
              and digest_stats["invalid_cache_entries"] == 1
              and digest_stats["cache_misses"] == 2)

        identity_checks = []
        for field, value in (
                ("bank_digest", "b" * 64), ("ticker", "BBB"),
                ("interval", "5m"), ("aggregation_version", 999),
                ("rth_window_version", 999)):
            document = read_document(path)
            document[field] = value
            write_document(path, document)
            _result, identity_stats = cache.derive_series(
                bank, "AAA", "1m", cache_root=cache_root)
            identity_checks.append(
                identity_stats["invalid_cache_entries"] == 1
                and identity_stats["cache_misses"] == 2)
        check("schema/root/ticker/interval mismatches fail closed",
              all(identity_checks))

        path.write_text("{truncated", encoding="utf-8")
        malformed, malformed_stats = cache.derive_series(
            bank, "AAA", "1m", cache_root=cache_root)
        check("malformed JSON falls back to complete source derivation",
              malformed == legacy_daily(bank)
              and malformed_stats["invalid_cache_entries"] == 1)

        valid = read_document(path)
        bad_record = copy.deepcopy(valid)
        bad_record["months"]["2024-01"]["records"][0] = ["2024-01-08", 3]
        expect_error("truncated daily record is rejected",
                     lambda: cache.validate_cache_document(
                         bad_record, bank_digest=valid["bank_digest"],
                         ticker="AAA", interval="1m"),
                     cache.CacheValidationError)

        too_many_days = copy.deepcopy(valid)
        too_many_days["months"]["2024-01"]["records"] = (
            too_many_days["months"]["2024-01"]["records"] * 32)
        expect_error("per-month day bound is enforced",
                     lambda: cache.validate_cache_document(
                         too_many_days, bank_digest=valid["bank_digest"],
                         ticker="AAA", interval="1m"),
                     cache.CacheValidationError)

        too_many_months = copy.deepcopy(valid)
        template = next(iter(too_many_months["months"].values()))
        month_map = {}
        year, number = 1900, 1
        for _index in range(cache.MAX_MONTHS + 1):
            key = f"{year:04d}-{number:02d}"
            month_map[key] = template
            number += 1
            if number == 13:
                year += 1
                number = 1
        too_many_months["months"] = month_map
        expect_error("document month bound is enforced",
                     lambda: cache.validate_cache_document(
                         too_many_months, bank_digest=valid["bank_digest"],
                         ticker="AAA", interval="1m"),
                     cache.CacheValidationError)

        old_bound = cache.MAX_CACHE_BYTES
        try:
            cache.MAX_CACHE_BYTES = 16
            path.write_bytes(b"x" * 17)
            bounded, bounded_stats = cache.derive_series(
                bank, "AAA", "1m", cache_root=cache_root)
        finally:
            cache.MAX_CACHE_BYTES = old_bound
        check("pre-parse byte bound falls back without partial data",
              bounded == legacy_daily(bank)
              and bounded_stats["invalid_cache_entries"] == 1)
    finally:
        shutil.rmtree(base, ignore_errors=True)


def test_source_trust_and_write_failures():
    base = Path(tempfile.mkdtemp(prefix="ddc_trust_"))
    try:
        # Legacy manifest: authoritative after full SHA, but not warm-cacheable.
        bank, cache_root = base / "legacy", base / "legacy-cache"
        paths = seed_series(
            bank, "AAA", "1m",
            {"2024-03": day_bars(date(2024, 3, 4), 3, 10.0)})
        manifest = storage.load_manifest(bank / "AAA")
        legacy_entry = manifest["intervals"]["1m"]["months"]["2024-03"]
        legacy_entry.pop("size")
        legacy_entry.pop("mtime_ns")
        storage.save_manifest(bank / "AAA", manifest)
        legacy, legacy_stats = cache.derive_series(
            bank, "AAA", "1m", cache_root=cache_root)
        document = read_document(legacy_stats["cache_path"])
        _legacy2, legacy_stats2 = cache.derive_series(
            bank, "AAA", "1m", cache_root=cache_root)
        check("legacy manifest is source-current but never cache-hit/persisted",
              legacy == legacy_daily(bank)
              and legacy_stats["source_current"] is True
              and legacy_stats["cacheable"] is False
              and document["months"] == {}
              and legacy_stats2["cache_hits"] == 0
              and legacy_stats2["cache_misses"] == 1)

        # Pure mtime touch: SHA/rows/bounds remain authoritative, stale stat is not cached.
        bank2, cache2 = base / "touch", base / "touch-cache"
        paths2 = seed_series(
            bank2, "AAA", "1m",
            {"2024-04": day_bars(date(2024, 4, 2), 3, 20.0)})
        expected2, cold2 = cache.derive_series(
            bank2, "AAA", "1m", cache_root=cache2)
        source_path = paths2["2024-04"]
        source_stat = source_path.stat()
        os.utime(source_path, ns=(source_stat.st_atime_ns,
                                  source_stat.st_mtime_ns + 10_000_000))
        touched, touched_stats = cache.derive_series(
            bank2, "AAA", "1m", cache_root=cache2)
        check("identical-byte mtime touch rehashes authoritatively but is not cached",
              touched == expected2 and touched_stats["cache_hits"] == 0
              and touched_stats["source_current"] is True
              and touched_stats["cacheable"] is False)

        # A manifest mutation after a warm lookup must be visible in source_current.
        bank2b, cache2b = base / "warm-race", base / "warm-race-cache"
        seed_series(
            bank2b, "AAA", "1m",
            {"2024-04": day_bars(date(2024, 4, 3), 3, 25.0)})
        warm_expected, _warm_cold_stats = cache.derive_series(
            bank2b, "AAA", "1m", cache_root=cache2b)
        original_stat = cache._stat_path
        warm_raced = [False]

        def warm_racing_stat(path):
            result = original_stat(path)
            if Path(path).suffix == ".csv" and not warm_raced[0]:
                warm_raced[0] = True
                current = storage.load_manifest(bank2b / "AAA")
                current["intervals"]["1m"]["months"]["2024-04"][
                    "race_marker"] = "changed-after-hit"
                storage.save_manifest(bank2b / "AAA", current)
            return result

        cache._stat_path = warm_racing_stat
        try:
            warm_race_daily, warm_race_stats = cache.derive_series(
                bank2b, "AAA", "1m", cache_root=cache2b)
        finally:
            cache._stat_path = original_stat
        check("warm manifest race cannot report current evidence",
              warm_race_daily == warm_expected
              and warm_race_stats["cache_hits"] == 1
              and warm_race_stats["source_current"] is False
              and warm_race_stats["cacheable"] is False)

        # Unmanifested source rewrite: output is usable for generic callers but non-current.
        bank3, cache3 = base / "rewrite", base / "rewrite-cache"
        paths3 = seed_series(
            bank3, "AAA", "1m",
            {"2024-05": day_bars(date(2024, 5, 6), 3, 30.0)})
        old_daily, _old_stats = cache.derive_series(
            bank3, "AAA", "1m", cache_root=cache3)
        storage.write_month_file(
            paths3["2024-05"], day_bars(date(2024, 5, 6), 4, 60.0))
        rewritten, rewritten_stats = cache.derive_series(
            bank3, "AAA", "1m", cache_root=cache3)
        check("stat-changing rewrite without manifest is never served warm",
              rewritten != old_daily
              and rewritten_stats["cache_hits"] == 0
              and rewritten_stats["source_current"] is False
              and rewritten_stats["cacheable"] is False)

        # Manifest mutation after read prevents publication under the captured identity.
        bank4, cache4 = base / "race", base / "race-cache"
        seed_series(
            bank4, "AAA", "1m",
            {"2024-06": day_bars(date(2024, 6, 3), 3, 40.0)})
        original_reader = cache._read_source_month
        raced = [False]

        def racing_reader(path, sha):
            result = original_reader(path, sha)
            if not raced[0]:
                raced[0] = True
                current = storage.load_manifest(bank4 / "AAA")
                current["intervals"]["1m"]["months"]["2024-06"][
                    "race_marker"] = "changed"
                storage.save_manifest(bank4 / "AAA", current)
            return result

        cache._read_source_month = racing_reader
        try:
            raced_daily, raced_stats = cache.derive_series(
                bank4, "AAA", "1m", cache_root=cache4)
        finally:
            cache._read_source_month = original_reader
        race_document = read_document(raced_stats["cache_path"])
        check("source/manifest race returns rows but persists no wrong entry",
              bool(raced_daily) and raced_stats["source_current"] is False
              and raced_stats["cacheable"] is False
              and race_document["months"] == {})

        # Atomic write/readback failure remains a source-successful call.
        bank5, cache5 = base / "writefail", base / "writefail-cache"
        seed_series(
            bank5, "AAA", "1m",
            {"2024-07": day_bars(date(2024, 7, 1), 3, 50.0)})
        original_writer = cache._atomic_write_cache
        cache._atomic_write_cache = lambda _path, _payload: None
        try:
            fallback, fallback_stats = cache.derive_series(
                bank5, "AAA", "1m", cache_root=cache5)
        finally:
            cache._atomic_write_cache = original_writer
        check("atomic write/readback failure leaves source output usable",
              fallback == legacy_daily(bank5)
              and fallback_stats["persistence_attempted"] is True
              and fallback_stats["persistence_succeeded"] is False)
    finally:
        shutil.rmtree(base, ignore_errors=True)


def test_collision_and_concurrency():
    base = Path(tempfile.mkdtemp(prefix="ddc_parallel_"))
    try:
        shared_cache = base / "cache"
        bank_a, bank_b = base / "bank-a", base / "bank-b"
        seed_series(bank_a, "AAA", "1m", {
            "2024-08": day_bars(date(2024, 8, 1), 3, 10.0)})
        seed_series(bank_b, "AAA", "1m", {
            "2024-08": day_bars(date(2024, 8, 1), 3, 90.0)})
        original_digest = cache._namespace_digest
        prefix = "1234567890abcdef"

        def colliding_digest(root):
            return prefix + (("a" if Path(root).name == "bank-a" else "b") * 48)

        cache._namespace_digest = colliding_digest
        try:
            daily_a, _stats_a = cache.derive_series(
                bank_a, "AAA", "1m", cache_root=shared_cache)
            daily_b, stats_b = cache.derive_series(
                bank_b, "AAA", "1m", cache_root=shared_cache)
            daily_a2, stats_a2 = cache.derive_series(
                bank_a, "AAA", "1m", cache_root=shared_cache)
        finally:
            cache._namespace_digest = original_digest
        check("namespace-prefix collision cannot cross-hit banks",
              daily_a == daily_a2 and daily_a != daily_b
              and stats_b["invalid_cache_entries"] == 1
              and stats_a2["invalid_cache_entries"] == 1)

        bank_c, cache_c = base / "bank-c", base / "cache-c"
        seed_series(bank_c, "AAA", "1m", {
            "2024-09": day_bars(date(2024, 9, 3), 4, 30.0),
            "2024-10": day_bars(date(2024, 10, 1), 4, 40.0),
        })
        expected = legacy_daily(bank_c)
        barrier = threading.Barrier(6)
        outputs, errors = [], []
        lock = threading.Lock()

        def worker():
            try:
                barrier.wait(timeout=5)
                result = cache.derive_series(
                    bank_c, "AAA", "1m", cache_root=cache_c)[0]
                with lock:
                    outputs.append(result)
            except Exception as exc:  # noqa: BLE001
                with lock:
                    errors.append(exc)

        threads = [threading.Thread(target=worker) for _ in range(6)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=20)
        final, final_stats = cache.derive_series(
            bank_c, "AAA", "1m", cache_root=cache_c)
        document = read_document(final_stats["cache_path"])
        validated = cache.validate_cache_document(
            document, bank_digest=cache._namespace_digest(bank_c),
            ticker="AAA", interval="1m")
        check("concurrent same-path calls remain complete and uncorrupted",
              not errors and len(outputs) == 6
              and all(output == expected for output in outputs)
              and final == expected and final_stats["cache_hits"] == 2
              and len(validated["months"]) == 2,
              repr(errors))
    finally:
        shutil.rmtree(base, ignore_errors=True)


def test_combined_integration():
    base = Path(tempfile.mkdtemp(prefix="ddc_integration_"))
    try:
        bank = base / "bank"
        bars = day_bars(date(2024, 11, 4), 4, 100.0)
        seed_series(bank, "AAA", "1m", {"2024-11": bars})
        derived = legacy_daily(bank)
        external = {
            day.isoformat(): values for day, values in derived.items()}
        internal = dict(derived)
        cold = validate.combined_crosscheck(
            bank, "AAA", "1m", ext_ref=external, int_ref=internal)
        check("uninjected combined check uses cache path without semantic change",
              cold["status"] == "ok" and not cold["persistent"]
              and "cache_stats" not in cold)

        original_reader = cache._read_source_month
        cache._read_source_month = lambda *_args: (_ for _ in ()).throw(
            AssertionError("warm combined check decoded source"))
        try:
            warm = validate.combined_crosscheck(
                bank, "AAA", "1m", ext_ref=external, int_ref=internal)
        finally:
            cache._read_source_month = original_reader
        check("warm combined check performs no month decode", warm == cold)

        original_derive = cache.derive_series
        cache.derive_series = lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("injected read path called cache"))
        try:
            injected = validate.combined_crosscheck(
                bank, "AAA", "1m", ext_ref=external, int_ref=internal,
                read_fn=lambda *_args: bars)
        finally:
            cache.derive_series = original_derive
        check("injected combined tests retain exact legacy read path",
              injected["status"] == "ok")

        cache.derive_series = lambda *_args, **_kwargs: (
            derived, {"source_current": False})
        try:
            raced = validate.combined_crosscheck_with_provenance(
                bank, "AAA", "1m",
                lambda: validate.combined_crosscheck(
                    bank, "AAA", "1m", ext_ref=external, int_ref=internal),
                fingerprint_fn=fixture_fingerprint)
        finally:
            cache.derive_series = original_derive
        check("non-current cache source cannot publish ok or flagged",
              raced["status"] == "error"
              and raced["interval_fingerprint"]["current"] is True
              and "SourceNotCurrentError" in raced.get("note", ""))
    finally:
        shutil.rmtree(base, ignore_errors=True)


def test_benchmark_runner():
    base = Path(tempfile.mkdtemp(prefix="ddc_benchmark_"))
    try:
        bank = base / "bank"
        cache_parent = base / "cache-parent"
        artifact_path = base / "artifact.json"
        gate_path = base / "operation.lock"
        seed_series(bank, "AAA", "1m", {
            "2024-12": day_bars(date(2024, 12, 2), 5, 100.0),
        })
        artifact, destination = benchmark.run_benchmark(
            bank, tickers=("AAA",), output=artifact_path,
            gate_path=gate_path, cache_parent=cache_parent)
        check("benchmark runner proves cold/warm structural gate",
              destination == artifact_path.resolve()
              and artifact["structural_gate"]["passed"] is True
              and artifact["cold"]["totals"]["source_rows_decoded"] == 5
              and artifact["warm"]["totals"]["source_rows_decoded"] == 0
              and artifact["source_row_reduction"] == 1.0)
        check("benchmark runner leaves bank metadata unchanged",
              artifact["bank_metadata_before"]
              == artifact["bank_metadata_after"])
        check("benchmark runner removes only its owned temporary cache",
              cache_parent.is_dir()
              and not list(cache_parent.glob("benchmark-*"))
              and artifact_path.is_file())

        busy_cache = base / "busy-cache"
        with operation_gate.acquire(
                "fetch", owner="benchmark selftest", path=gate_path):
            expect_error(
                "busy operation gate blocks benchmark before cache writes",
                lambda: benchmark.run_benchmark(
                    bank, tickers=("AAA",), output=base / "blocked.json",
                    gate_path=gate_path, cache_parent=busy_cache),
                operation_gate.OperationBusy)
        check("blocked benchmark creates no cache directory",
              not busy_cache.exists())
    finally:
        shutil.rmtree(base, ignore_errors=True)


def main():
    test_cold_warm_and_edges()
    test_invalidation_and_bounds()
    test_source_trust_and_write_failures()
    test_collision_and_concurrency()
    test_combined_integration()
    test_benchmark_runner()
    print(f"\nderived-daily cache: {CHECKS[0] - len(FAILS)}/{CHECKS[0]} passed")
    if FAILS:
        print("failed:", ", ".join(FAILS))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
