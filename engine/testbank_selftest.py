"""Offline contract tests for the shared synthetic-bank and gate-isolation kit."""

from __future__ import annotations

import ast
import contextlib
import copy
import datetime as dt
import hashlib
import importlib
import json
import os
import subprocess
import sys
import tempfile
import threading
import types
from pathlib import Path

ENGINE_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = ENGINE_ROOT.parent
sys.path.insert(0, str(ENGINE_ROOT))
sys.path.insert(0, str(PROJECT_ROOT))

import operation_gate  # noqa: E402
import stock_storage as storage  # noqa: E402
import testbank  # noqa: E402


FAILURES = []
COUNT = [0]


def check(name, condition, detail=""):
    COUNT[0] += 1
    print(f"[{'PASS' if condition else 'FAIL'}] {name}"
          + (f" -- {detail}" if detail and not condition else ""))
    if not condition:
        FAILURES.append(name)


def expect_error(name, fn, error=testbank.TestBankError):
    try:
        fn()
    except error:
        check(name, True)
    except BaseException as exc:  # noqa: BLE001
        check(name, False, f"wrong exception {type(exc).__name__}: {exc}")
    else:
        check(name, False, "did not raise")


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def first_weekday(year, month):
    day = dt.date(year, month, 1)
    while day.weekday() >= 5:
        day += dt.timedelta(days=1)
    return day


def month_keys(count):
    return [f"{2023 + index // 12:04d}-{1 + index % 12:02d}"
            for index in range(count)]


def data_tree_bytes(root):
    root = Path(root)
    return {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in sorted(root.rglob("*"))
        if path.is_file() and path.name != storage.MANIFEST_NAME
    }


def logical_manifest(path):
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    for interval in payload.get("intervals", {}).values():
        for entry in interval.get("months", {}).values():
            entry.pop("mtime_ns", None)
    return payload


def reference_seed_bank(root, geometry):
    """Exact independent transcription of export_progress_reference.seed_bank."""
    for offset, (ticker, count) in enumerate(sorted(geometry.items())):
        ticker_dir = Path(root) / ticker
        ticker_dir.mkdir(parents=True)
        manifest = storage.new_manifest(ticker, ticker)
        manifest["conid"] = 20_000 + sum(ord(char) for char in ticker)
        months = storage.manifest_months(manifest, "1m")
        for index, key in enumerate(month_keys(count)):
            year, month = map(int, key.split("-"))
            day = first_weekday(year, month)
            base = 10.0 + offset + index * 0.25
            bars = [
                (dt.datetime(year, month, day.day, 9, 30),
                 base, base + 1.0, base - 0.5, base + 0.5, 100 + index),
                (dt.datetime(year, month, day.day, 9, 31),
                 base + 0.5, base + 1.5, base, base + 1.0, 200 + index),
            ]
            path = storage.month_file_path(
                root, ticker, year, month, "1m", fmt="csv")
            months[key] = storage.write_month_file(path, bars)
        storage.save_manifest(ticker_dir, manifest)


def test_reference_equivalence():
    geometry = {
        "AAA": 1, "BBB": 2, "CCC": 4,
        "DDD": 8, "EEE": 16, "FFF": 32,
    }
    spec = {ticker: {"1m": month_keys(count)}
            for ticker, count in reversed(list(geometry.items()))}
    with tempfile.TemporaryDirectory(prefix="testbank-equivalence-") as temp:
        reference = Path(temp) / "reference"
        shared = Path(temp) / "shared"
        reference_seed_bank(reference, geometry)
        actual = testbank.build_bank(shared, spec)
        check("D1 matches reference month paths and bytes exactly",
              data_tree_bytes(reference) == data_tree_bytes(shared))

        legacy_without_status = True
        logical_match_after_upgrade = True
        for ticker in sorted(geometry):
            legacy = logical_manifest(
                reference / ticker / storage.MANIFEST_NAME)
            generated = logical_manifest(
                shared / ticker / storage.MANIFEST_NAME)
            upgraded = copy.deepcopy(legacy)
            for interval in upgraded["intervals"].values():
                for entry in interval["months"].values():
                    legacy_without_status &= "status" not in entry
                    entry["status"] = "present"
            logical_match_after_upgrade &= upgraded == generated
        check("reference transcription preserves its legacy status omission",
              legacy_without_status)
        check("D1's only logical manifest delta is canonical present status",
              logical_match_after_upgrade)
        check("returned fingerprints equal each raw manifest",
              actual == {
                  ticker: sha(shared / ticker / storage.MANIFEST_NAME)
                  for ticker in sorted(geometry)
              })

        for ticker in sorted(geometry):
            manifest = storage.load_manifest(reference / ticker)
            for interval in manifest["intervals"].values():
                for entry in interval["months"].values():
                    entry["status"] = "present"
            storage.save_manifest(reference / ticker, manifest)
        fingerprints_match = all(
            storage.interval_state_fingerprint(reference, ticker, "1m")
            ["sha256"]
            == storage.interval_state_fingerprint(shared, ticker, "1m")
            ["sha256"]
            for ticker in geometry)
        check("canonical fingerprints match after the required status upgrade",
              fingerprints_match)


def test_input_order_determinism():
    first_spec = {
        "ZZZ": {"1m-post": ["2026-02", "2026-01"],
                "1m": ["2026-03"]},
        "AAA": {"1d-iv": ["2026-02"], "1m": ["2026-01"]},
    }
    second_spec = {
        "AAA": {"1m": ["2026-01"], "1d-iv": ["2026-02"]},
        "ZZZ": {"1m": ["2026-03"],
                "1m-post": ["2026-01", "2026-02"]},
    }
    with tempfile.TemporaryDirectory(prefix="testbank-order-") as temp:
        first = Path(temp) / "first"
        second = Path(temp) / "second"
        first_hashes = testbank.build_bank(first, first_spec)
        second_hashes = testbank.build_bank(second, second_spec)
        check("equivalent input order produces identical month bytes",
              data_tree_bytes(first) == data_tree_bytes(second))
        check("equivalent input order produces identical logical manifests",
              all(
                  logical_manifest(first / ticker / storage.MANIFEST_NAME)
                  == logical_manifest(second / ticker / storage.MANIFEST_NAME)
                  for ticker in ("AAA", "ZZZ")))
        check("input order preserves sorted return keys and self hashes",
              list(first_hashes) == list(second_hashes) == ["AAA", "ZZZ"]
              and all(
                  hashes[ticker]
                  == sha(root / ticker / storage.MANIFEST_NAME)
                  for hashes, root in ((first_hashes, first),
                                       (second_hashes, second))
                  for ticker in hashes))


def test_mixed_shapes_and_storage_contract():
    spec = {
        "ZZZ": {
            "1m-post": ["2026-02"],
            "1d-hvol": ["2026-01"],
            "1m-iv": ["2026-01"],
            "1m-bidask-pre": ["2026-01"],
            "1d-iv": ["2026-02"],
            "1m": ["2026-02", "2026-01"],
        },
        "AAA": {"1m": ["2026-01"]},
    }
    with tempfile.TemporaryDirectory(prefix="testbank-shapes-") as temp:
        root = Path(temp) / "bank"
        fingerprints = testbank.build_bank(root, spec)
        check("fingerprint mapping uses sorted canonical tickers",
              list(fingerprints) == ["AAA", "ZZZ"])
        check("manifests carry deterministic conids",
              storage.load_manifest(root / "ZZZ")["conid"]
              == 20_000 + sum(ord(char) for char in "ZZZ"))

        valid_entries = True
        canonical_intervals = True
        for ticker, intervals in spec.items():
            manifest = storage.load_manifest(root / ticker)
            for interval, keys in intervals.items():
                try:
                    fp = storage.interval_state_fingerprint(
                        root, ticker, interval)
                    canonical_intervals &= fp["month_count"] == len(keys)
                except Exception:  # noqa: BLE001
                    canonical_intervals = False
                for key in keys:
                    year, month = map(int, key.split("-"))
                    entry = storage.manifest_months(manifest, interval)[key]
                    path = storage.month_file_path(
                        root, ticker, year, month, interval, fmt="csv")
                    _bars, stats = storage.read_month_file(path)
                    valid_entries &= (
                        entry.get("status") == "present"
                        and entry.get("sha256") == stats.get("sha256")
                        and entry.get("rows") == 2)
        check("every month is canonical and SHA-linked to real bytes",
              valid_entries)
        check("every generated interval passes provenance fingerprinting",
              canonical_intervals)
        check("returned raw hashes match saved manifests",
              all(fingerprint == sha(root / ticker / storage.MANIFEST_NAME)
                  for ticker, fingerprint in fingerprints.items()))

        def read(interval, key):
            year, month = map(int, key.split("-"))
            path = storage.month_file_path(
                root, "ZZZ", year, month, interval, fmt="csv")
            return storage.read_month_file(path)[0]

        ordinary = read("1m", "2026-01")
        pre = read("1m-bidask-pre", "2026-01")
        post = read("1m-post", "2026-02")
        daily = read("1d-iv", "2026-02")
        ratio = (read("1m-iv", "2026-01")
                 + daily + read("1d-hvol", "2026-01"))
        check("ordinary intraday rows use first-weekday RTH minutes",
              [bar[0].time() for bar in ordinary]
              == [dt.time(9, 30), dt.time(9, 31)]
              and ordinary[0][5] > 0)
        check("pre/post tokens use their own session starts",
              [bar[0].time() for bar in pre]
              == [dt.time(4, 0), dt.time(4, 1)]
              and [bar[0].time() for bar in post]
              == [dt.time(16, 0), dt.time(16, 1)]
              and pre[0][0].date() == first_weekday(2026, 1)
              and post[0][0].date() == first_weekday(2026, 2))
        check("bidask uses stored sentinel volume",
              all(bar[5] == 0 for bar in pre))
        check("daily rows use midnight on consecutive weekdays",
              all(bar[0].time() == dt.time() for bar in daily)
              and all(bar[0].weekday() < 5 for bar in daily)
              and daily[0][0].date() == first_weekday(2026, 2)
              and (daily[1][0].date() - daily[0][0].date()).days <= 3)
        check("IV/HVOL rows are ratio-shaped with sentinel volume",
              bool(ratio) and all(
                  0 < value < 1
                  for bar in ratio for value in bar[1:5])
              and all(bar[5] == 0 for bar in ratio))


def test_parquet_round_trip():
    with tempfile.TemporaryDirectory(prefix="testbank-parquet-") as temp:
        root = Path(temp) / "bank"
        result = testbank.build_bank(
            root,
            {"AAA": {"1m": ["2026-01"], "1d-iv": ["2026-02"]}},
            bars_per_day=1,
            fmt="parquet",
        )
        paths = sorted(root.rglob("*.parquet"))
        decoded = [storage.read_month_file(path)[0] for path in paths]
        manifest = storage.load_manifest(root / "AAA")
        slots = [
            (storage.manifest_months(manifest, "1m")["2026-01"],
             storage.month_file_path(
                 root, "AAA", 2026, 1, "1m", fmt="parquet")),
            (storage.manifest_months(manifest, "1d-iv")["2026-02"],
             storage.month_file_path(
                 root, "AAA", 2026, 2, "1d-iv", fmt="parquet")),
        ]
        check("parquet fixture path and codec round-trip are supported",
              len(paths) == 2 and all(len(rows) == 1 for rows in decoded))
        check("parquet manifests carry present status and matching file SHA",
              all(entry["status"] == "present"
                  and entry["sha256"] == sha(path)
                  for entry, path in slots))
        check("parquet intervals pass canonical provenance fingerprinting",
              all(storage.interval_state_fingerprint(
                  root, "AAA", interval)["month_count"] == 1
                  for interval in ("1m", "1d-iv")))
        check("parquet build returns its raw manifest fingerprint",
              result["AAA"] == sha(root / "AAA" / storage.MANIFEST_NAME))


def test_success_boundaries_and_canonicalization():
    with tempfile.TemporaryDirectory(prefix="testbank-boundaries-") as temp:
        root = Path(temp) / "bank"
        result = testbank.build_bank(
            root,
            {"brk.b": {"1m": ["2100-12", "1900-01"]}},
            bars_per_day=3,
        )
        manifest = storage.load_manifest(root / "BRK-B")
        rows = []
        for year, month in ((1900, 1), (2100, 12)):
            path = storage.month_file_path(
                root, "BRK-B", year, month, "1m", fmt="csv")
            rows.append(storage.read_month_file(path)[0])
        check("canonical ticker spelling preserves the source symbol",
              list(result) == ["BRK-B"]
              and manifest["folder"] == "BRK-B"
              and manifest["symbol"] == "BRK.B")
        check("inclusive storage year boundaries build successfully",
              all(len(group) == 3 for group in rows)
              and rows[0][0][0].year == 1900
              and rows[1][0][0].year == 2100)
        check("bars_per_day above the default yields consecutive minutes",
              all([bar[0].time() for bar in group]
                  == [dt.time(9, 30), dt.time(9, 31), dt.time(9, 32)]
                  for group in rows))


def test_validation_is_prewrite_and_non_destructive():
    good = {"AAA": {"1m": ["2026-01"]}}
    cases = [
        ("empty spec", {}, 2, "csv"),
        ("nonmapping spec", [], 2, "csv"),
        ("invalid ticker", {"!!!": {"1m": ["2026-01"]}}, 2, "csv"),
        ("canonical ticker collision", {
            "BRK.B": {"1m": ["2026-01"]},
            "BRK-B": {"1m": ["2026-02"]}}, 2, "csv"),
        ("empty interval map", {"AAA": {}}, 2, "csv"),
        ("invalid interval", {"AAA": {"01m": ["2026-01"]}}, 2, "csv"),
        ("empty months", {"AAA": {"1m": []}}, 2, "csv"),
        ("invalid month", {"AAA": {"1m": ["2026-1"]}}, 2, "csv"),
        ("year below storage range", {
            "AAA": {"1m": ["1899-12"]}}, 2, "csv"),
        ("year above storage range", {
            "AAA": {"1m": ["2101-01"]}}, 2, "csv"),
        ("duplicate month", {
            "AAA": {"1m": ["2026-01", "2026-01"]}}, 2, "csv"),
        ("boolean bar count", good, True, "csv"),
        ("zero bar count", good, 0, "csv"),
        ("session overflow", {"AAA": {"1m-pre": ["2026-01"]}},
         331, "csv"),
        ("daily month overflow", {"AAA": {"1d": ["2026-02"]}},
         32, "csv"),
        ("invalid format", good, 2, "json"),
        ("unhashable format", good, 2, []),
        ("synthetic conid collision", {
            "AB": {"1m": ["2026-01"]},
            "BA": {"1m": ["2026-01"]}}, 2, "csv"),
    ]
    with tempfile.TemporaryDirectory(prefix="testbank-invalid-") as temp:
        empty = Path(temp) / "existing-empty"
        empty.mkdir()
        empty_result = testbank.build_bank(empty, good)
        check("an existing empty fixture root is supported",
              empty_result["AAA"]
              == sha(empty / "AAA" / storage.MANIFEST_NAME))

        for index, (label, spec, count, fmt) in enumerate(cases):
            root = Path(temp) / f"case-{index}"
            expect_error(
                f"{label} is rejected before creating the fixture root",
                lambda root=root, spec=spec, count=count, fmt=fmt:
                    testbank.build_bank(
                        root, spec, bars_per_day=count, fmt=fmt))
            check(f"{label} leaves no partial tree", not root.exists())

        occupied = Path(temp) / "occupied"
        occupied.mkdir()
        sentinel = occupied / "keep.txt"
        sentinel.write_text("keep", encoding="ascii")
        expect_error(
            "nonempty roots are never merged or overwritten",
            lambda: testbank.build_bank(occupied, good))
        check("nonempty-root rejection preserves prior bytes",
              sentinel.read_bytes() == b"keep"
              and list(occupied.iterdir()) == [sentinel])

        partial = Path(temp) / "partial"
        expect_error(
            "late-sorted invalid years are rejected before earlier tickers",
            lambda: testbank.build_bank(partial, {
                "AAA": {"1m": ["2026-01"]},
                "ZZZ": {"1m": ["2101-01"]},
            }))
        check("multi-ticker prevalidation leaves no partial root",
              not partial.exists())


@contextlib.contextmanager
def surrogate_unsafe_roots(project, engine, bank):
    originals = (testbank._PROJECT_ROOT, testbank._ENGINE_ROOT,
                 testbank._PRODUCTION_BANK)
    testbank._PROJECT_ROOT = Path(project).resolve()
    testbank._ENGINE_ROOT = Path(engine).resolve()
    testbank._PRODUCTION_BANK = Path(bank).resolve()
    try:
        yield
    finally:
        (testbank._PROJECT_ROOT, testbank._ENGINE_ROOT,
         testbank._PRODUCTION_BANK) = originals


def test_unsafe_root_containment():
    good = {"AAA": {"1m": ["2026-01"]}}
    with tempfile.TemporaryDirectory(prefix="testbank-root-guard-") as temp:
        base = Path(temp)
        project = base / "fake-project"
        engine = base / "fake-engine"
        production = base / "fake-production-bank"
        production.mkdir()
        alias = base / "production-alias"
        try:
            alias.symlink_to(production, target_is_directory=True)
            alias_kind = "symlink"
        except OSError:
            alias_kind = None
            if os.name == "nt":
                made = subprocess.run(
                    ["cmd", "/c", "mklink", "/J", str(alias),
                     str(production)],
                    capture_output=True,
                    text=True,
                    check=False,
                    timeout=10,
                )
                if made.returncode == 0:
                    alias_kind = "junction"
        check("root guard test created a resolvable filesystem alias",
              alias_kind in {"symlink", "junction"}
              and alias.resolve() == production.resolve())
        candidates = [
            ("exact project root", project),
            ("project descendant", project / "fixtures"),
            ("engine descendant", engine / "fixtures"),
            ("exact production root", production),
            ("production descendant", production / "fixtures"),
            ("resolved filesystem-alias production descendant",
             alias / "fixtures"),
        ]
        with surrogate_unsafe_roots(project, engine, production):
            for label, candidate in candidates:
                expect_error(
                    f"{label} is refused by explicit containment",
                    lambda candidate=candidate: testbank.build_bank(
                        candidate, good))
        check("unsafe-root regressions create no synthetic ticker tree",
              not any((candidate / "AAA").exists()
                      for _label, candidate in candidates)
              and not (production / "fixtures").exists())


def _same_path(left, right):
    return os.path.normcase(str(Path(left).resolve())) == os.path.normcase(
        str(Path(right).resolve()))


def gate_module(name, lock_path=None):
    module = types.ModuleType(name)
    module.LOCK_PATH = (Path(f"{name}-original.lock")
                        if lock_path is None else lock_path)
    return module


def dual_import_modules():
    return (importlib.import_module("engine.testbank"),
            importlib.import_module("engine.operation_gate"))


def test_foreign_gate_identity_fails_closed():
    script = r'''
import pathlib
import sys
import types

root = pathlib.Path(sys.argv[1]).resolve()
sys.path.insert(0, str(root))
foreign = types.ModuleType("operation_gate")
foreign.__file__ = str(root.parent / "foreign" / "operation_gate.py")
foreign.LOCK_PATH = pathlib.Path("FOREIGN-PRODUCTION.lock")
sys.modules["operation_gate"] = foreign

import engine.operation_gate as local_gate
import engine.testbank as package_testbank

local_original = local_gate.LOCK_PATH
try:
    with package_testbank.isolated_gates():
        raise AssertionError("foreign identity was not rejected")
except package_testbank.GateIsolationError as exc:
    if "foreign source" not in str(exc):
        raise
else:
    raise AssertionError("foreign identity did not fail closed")
assert local_gate.LOCK_PATH is local_original
assert foreign.LOCK_PATH == pathlib.Path("FOREIGN-PRODUCTION.lock")
print("FOREIGN_IDENTITY_REJECTED")
'''
    result = subprocess.run(
        [sys.executable, "-c", script, str(PROJECT_ROOT)],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )
    check("foreign canonical gate identities fail before any mutation",
          result.returncode == 0
          and result.stdout.strip() == "FOREIGN_IDENTITY_REJECTED",
          result.stderr or result.stdout)


def test_gate_success_exception_and_nesting():
    original = operation_gate.LOCK_PATH
    extra_original = "extra-original.lock"
    extra = gate_module("extra_gate", extra_original)
    parent = None
    with testbank.isolated_gates((extra, "LOCK_PATH")) as gate:
        parent = gate.parent
        check("gate context yields an absolute temporary path",
              gate.is_absolute() and parent.is_dir())
        check("default and extra targets share the sandbox path",
              operation_gate.LOCK_PATH == gate and extra.LOCK_PATH == gate)
        with operation_gate.acquire("testbank_fixture") as lease:
            check("implicit acquire resolves only to the sandbox gate",
                  _same_path(lease.path, gate))
        operation_gate.LOCK_PATH = Path("body-mutated-default")
        extra.LOCK_PATH = Path("body-mutated-extra")
    check("successful gate isolation restores exact originals",
          operation_gate.LOCK_PATH is original
          and extra.LOCK_PATH is extra_original)
    check("gate temp directory is removed after restore",
          parent is not None and not parent.exists())

    marker = RuntimeError("body sentinel")
    caught = None
    exception_parent = None
    exception_extra = gate_module("exception_extra")
    exception_extra_original = exception_extra.LOCK_PATH
    try:
        with testbank.isolated_gates(
                (exception_extra, "LOCK_PATH")) as gate:
            exception_parent = gate.parent
            raise marker
    except RuntimeError as exc:
        caught = exc
    check("body exceptions propagate unchanged after restoration",
          caught is marker
          and operation_gate.LOCK_PATH is original
          and exception_extra.LOCK_PATH is exception_extra_original)
    check("exception path removes the temporary directory",
          exception_parent is not None and not exception_parent.exists())

    class StopFixture(BaseException):
        pass

    try:
        with testbank.isolated_gates():
            raise StopFixture("stop")
    except StopFixture:
        pass
    check("BaseException paths also restore the production attribute",
          operation_gate.LOCK_PATH is original)

    nested_extra = gate_module("nested_extra", Path("nested-original"))
    with testbank.isolated_gates((nested_extra, "LOCK_PATH")) as outer:
        with testbank.isolated_gates() as inner:
            inner_ok = (inner != outer
                        and operation_gate.LOCK_PATH == inner
                        and nested_extra.LOCK_PATH == inner)
        outer_ok = (operation_gate.LOCK_PATH == outer
                    and nested_extra.LOCK_PATH == outer)
    check("nested contexts inherit aliases and restore inner to outer",
          inner_ok and outer_ok
          and operation_gate.LOCK_PATH is original
          and nested_extra.LOCK_PATH == Path("nested-original"))


def test_gate_validation_transactionality_and_threads():
    original = operation_gate.LOCK_PATH
    duplicate = gate_module("duplicate", Path("duplicate-original"))
    with testbank.isolated_gates(
            (duplicate, "LOCK_PATH"),
            (duplicate, "LOCK_PATH")) as gate:
        duplicate_ok = duplicate.LOCK_PATH == gate
    check("duplicate gate slots are patched and restored exactly once",
          duplicate_ok
          and duplicate.LOCK_PATH == Path("duplicate-original"))

    missing = types.ModuleType("missing_gate")
    expect_error(
        "invalid gate targets fail before any mutation",
        lambda: contextlib.ExitStack().enter_context(
            testbank.isolated_gates((missing, "LOCK_PATH"))),
        testbank.GateIsolationError)
    check("gate target validation leaves the default unchanged",
          operation_gate.LOCK_PATH is original)

    expect_error(
        "non-module gate targets fail closed before mutation",
        lambda: contextlib.ExitStack().enter_context(
            testbank.isolated_gates(
                (types.SimpleNamespace(LOCK_PATH="unsafe"), "LOCK_PATH"))),
        testbank.GateIsolationError)

    class MutateThenReject(types.ModuleType):
        def __init__(self):
            super().__init__("mutate_then_reject")
            types.ModuleType.__setattr__(
                self, "LOCK_PATH", "reject-original")
            types.ModuleType.__setattr__(self, "armed", True)

        def __setattr__(self, name, value):
            if name == "LOCK_PATH" and self.armed and isinstance(value, Path):
                types.ModuleType.__setattr__(self, name, value)
                raise RuntimeError("reject patch")
            types.ModuleType.__setattr__(self, name, value)

    first = gate_module("first_gate", "first-original")
    reject = MutateThenReject()
    expect_error(
        "mid-patch failure rolls back every previously changed slot",
        lambda: contextlib.ExitStack().enter_context(
            testbank.isolated_gates(
                (first, "LOCK_PATH"), (reject, "LOCK_PATH"))),
        testbank.GateIsolationError)
    check("transactional gate rollback restores exact prior values",
          operation_gate.LOCK_PATH is original
          and first.LOCK_PATH == "first-original"
          and reject.LOCK_PATH == "reject-original")

    class RejectRestore(types.ModuleType):
        def __init__(self):
            super().__init__("reject_restore")
            types.ModuleType.__setattr__(self, "original", "restore-original")
            types.ModuleType.__setattr__(self, "LOCK_PATH", self.original)
            types.ModuleType.__setattr__(self, "patched", False)

        def __setattr__(self, name, value):
            if name == "LOCK_PATH" and isinstance(value, Path):
                types.ModuleType.__setattr__(self, name, value)
                types.ModuleType.__setattr__(self, "patched", True)
                return
            if (name == "LOCK_PATH" and self.patched
                    and value is self.original):
                raise RuntimeError("reject restore")
            types.ModuleType.__setattr__(self, name, value)

    survivor = gate_module("restore_survivor", "survivor-original")
    hostile = RejectRestore()
    restore_error = None
    try:
        with testbank.isolated_gates(
                (hostile, "LOCK_PATH"), (survivor, "LOCK_PATH")):
            pass
    except testbank.GateIsolationError as exc:
        restore_error = exc
    check("restore failures are fatal, detailed, and do not skip other slots",
          restore_error is not None
          and "reject_restore.LOCK_PATH" in str(restore_error)
          and "RuntimeError: reject restore" in str(restore_error)
          and survivor.LOCK_PATH == "survivor-original"
          and operation_gate.LOCK_PATH is original)
    types.ModuleType.__setattr__(hostile, "patched", False)
    types.ModuleType.__setattr__(hostile, "LOCK_PATH", hostile.original)

    class InterruptAfterAppend(list):
        def append(self, item):
            super().append(item)
            raise KeyboardInterrupt("stack push interrupted")

    original_stack = testbank._GATE_TARGET_STACK
    interrupted_stack = InterruptAfterAppend()
    testbank._GATE_TARGET_STACK = interrupted_stack
    push_error = None
    try:
        with testbank.isolated_gates():
            pass
    except testbank.GateIsolationError as exc:
        push_error = exc
    finally:
        testbank._GATE_TARGET_STACK = original_stack
    check("BaseException during stack push rolls back paths and stack",
          push_error is not None
          and isinstance(push_error.__cause__, KeyboardInterrupt)
          and not interrupted_stack
          and operation_gate.LOCK_PATH is original)

    class InterruptSecondAppend(list):
        def __init__(self):
            super().__init__()
            self.calls = 0

        def append(self, item):
            self.calls += 1
            if self.calls == 2:
                raise KeyboardInterrupt("nested push interrupted")
            super().append(item)

    nested_stack = InterruptSecondAppend()
    testbank._GATE_TARGET_STACK = nested_stack
    nested_push_safe = False
    try:
        with testbank.isolated_gates() as outer_gate:
            try:
                with testbank.isolated_gates():
                    pass
            except testbank.GateIsolationError as exc:
                nested_push_safe = (
                    isinstance(exc.__cause__, KeyboardInterrupt)
                    and len(nested_stack) == 1
                    and operation_gate.LOCK_PATH == outer_gate)
    finally:
        testbank._GATE_TARGET_STACK = original_stack
    check("failed nested push never pops the outer stack frame",
          nested_push_safe and not nested_stack
          and operation_gate.LOCK_PATH is original)

    package_testbank, package_gate = dual_import_modules()
    package_original = package_gate.LOCK_PATH
    check("direct and package kit imports share one gate coordinator",
          package_testbank is not testbank
          and package_testbank._GATE_REDIRECT_LOCK
          is testbank._GATE_REDIRECT_LOCK
          and package_testbank._GATE_TARGET_STACK
          is testbank._GATE_TARGET_STACK)
    with package_testbank.isolated_gates() as dual_gate:
        dual_patched = (operation_gate.LOCK_PATH == dual_gate
                        and package_gate.LOCK_PATH == dual_gate)
    check("each kit identity patches both operation-gate module identities",
          dual_patched
          and operation_gate.LOCK_PATH is original
          and package_gate.LOCK_PATH is package_original)
    with testbank.isolated_gates() as cross_outer:
        with package_testbank.isolated_gates() as cross_inner:
            cross_inner_ok = (
                cross_inner != cross_outer
                and operation_gate.LOCK_PATH == cross_inner
                and package_gate.LOCK_PATH == cross_inner)
        cross_outer_ok = (operation_gate.LOCK_PATH == cross_outer
                          and package_gate.LOCK_PATH == cross_outer)
    check("cross-import nesting shares stack state and restores by layer",
          cross_inner_ok and cross_outer_ok
          and operation_gate.LOCK_PATH is original
          and package_gate.LOCK_PATH is package_original)

    a_entered = threading.Event()
    b_lock_attempted = threading.Event()
    b_entered = threading.Event()
    release_a = threading.Event()
    paths = {}
    errors = []

    def first_worker():
        try:
            with testbank.isolated_gates() as gate:
                paths["a"] = gate
                paths["a_dual"] = (
                    operation_gate.LOCK_PATH == gate
                    and package_gate.LOCK_PATH == gate)
                a_entered.set()
                if not release_a.wait(3):
                    raise RuntimeError("release timeout")
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    shared_lock = testbank._GATE_REDIRECT_LOCK

    class AttemptProbeLock:
        def __enter__(self):
            b_lock_attempted.set()
            shared_lock.acquire()
            return self

        def __exit__(self, exc_type, exc, traceback):
            shared_lock.release()

    package_lock = package_testbank._GATE_REDIRECT_LOCK
    package_testbank._GATE_REDIRECT_LOCK = AttemptProbeLock()

    def second_worker():
        try:
            if not a_entered.wait(3):
                raise RuntimeError("first entry timeout")
            with package_testbank.isolated_gates() as gate:
                paths["b"] = gate
                paths["b_dual"] = (
                    operation_gate.LOCK_PATH == gate
                    and package_gate.LOCK_PATH == gate)
                b_entered.set()
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    first_thread = threading.Thread(target=first_worker)
    second_thread = threading.Thread(target=second_worker)
    started_threads = []
    try:
        first_thread.start()
        started_threads.append(first_thread)
        second_thread.start()
        started_threads.append(second_thread)
        reached = a_entered.wait(3) and b_lock_attempted.wait(3)
        serialized = reached and not b_entered.is_set()
        release_a.set()
        first_thread.join(4)
        second_thread.join(4)
        completed = (
            not first_thread.is_alive() and not second_thread.is_alive()
            and b_entered.is_set())
    finally:
        release_a.set()
        for thread in started_threads:
            thread.join(4)
        package_testbank._GATE_REDIRECT_LOCK = package_lock
    check("cross-thread gate contexts serialize for their full bodies",
          serialized and completed and not errors, repr(errors))
    check("serialized threads receive distinct sandboxes and restore default",
          paths.get("a") != paths.get("b")
          and paths.get("a_dual") is True
          and paths.get("b_dual") is True
          and operation_gate.LOCK_PATH is original
          and package_gate.LOCK_PATH is package_original)


KIT_NAMES = {"testbank", "event_probe", "run_gates", "check_kit"}


def names_a_kit(name):
    return isinstance(name, str) and bool(
        set(name.split(".")) & KIT_NAMES)


def imported_kits(path):
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    constants = {}
    for node in ast.walk(tree):
        if (isinstance(node, (ast.Assign, ast.AnnAssign))
                and isinstance(node.value, ast.Constant)
                and isinstance(node.value.value, str)):
            targets = (node.targets if isinstance(node, ast.Assign)
                       else [node.target])
            for target in targets:
                if isinstance(target, ast.Name):
                    constants[target.id] = node.value.value
    found = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if names_a_kit(alias.name):
                    found.append((node.lineno, alias.name))
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            if names_a_kit(module):
                found.append((node.lineno, module))
            for alias in node.names:
                if alias.name in KIT_NAMES:
                    found.append((node.lineno, f"{module}:{alias.name}"))
        elif isinstance(node, ast.Call) and node.args:
            name = None
            if isinstance(node.func, ast.Name) and node.func.id == "__import__":
                name = node.args[0]
            elif (isinstance(node.func, ast.Attribute)
                  and node.func.attr == "import_module"):
                name = node.args[0]
            if (isinstance(name, ast.Constant)
                    and isinstance(name.value, str)
                    and names_a_kit(name.value)):
                found.append((node.lineno, name.value))
            elif (isinstance(name, ast.Name)
                  and names_a_kit(constants.get(name.id))):
                found.append((node.lineno, constants[name.id]))
    return found


def test_no_production_imports():
    candidates = list(PROJECT_ROOT.glob("*.py"))
    candidates.extend((PROJECT_ROOT / "engine").rglob("*.py"))
    kit_files = {f"{name}.py" for name in KIT_NAMES}

    def test_only(path):
        name = path.name
        return (name in kit_files
                or path.stem.endswith((
                    "_selftest", "_reference", "_test", "_bench")))

    violations = []
    for path in sorted(candidates):
        if test_only(path):
            continue
        for line, imported in imported_kits(path):
            violations.append(f"{path.relative_to(PROJECT_ROOT)}:{line}:{imported}")
    check("no production module in known source roots imports any kit",
          not violations, "; ".join(violations[:10]))
    check("testbank publishes a grep-able TEST_ONLY sentinel",
          testbank.TEST_ONLY is True)

    with tempfile.TemporaryDirectory(prefix="testbank-import-pin-") as temp:
        fixture = Path(temp) / "production_shape.py"
        fixture.write_text(
            "import engine.testbank.helpers\n"
            "from engine import event_probe\n"
            "from engine.run_gates import main\n"
            "import importlib\n"
            "importlib.import_module('engine.check_kit.helpers')\n"
            "__import__('vendor.testbank.helpers')\n"
            "KIT_TARGET = 'engine.event_probe.helpers'\n"
            "importlib.import_module(KIT_TARGET)\n",
            encoding="utf-8",
        )
        found = imported_kits(fixture)
    expected_imports = sorted([
        "engine.testbank.helpers",
        "engine:event_probe",
        "engine.run_gates",
        "engine.check_kit.helpers",
        "vendor.testbank.helpers",
        "engine.event_probe.helpers",
    ])
    check("import scanner catches nested static and literal dynamic forms",
          sorted(imported for _line, imported in found) == expected_imports,
          repr(found))


def run():
    test_reference_equivalence()
    test_input_order_determinism()
    test_mixed_shapes_and_storage_contract()
    test_parquet_round_trip()
    test_success_boundaries_and_canonicalization()
    test_validation_is_prewrite_and_non_destructive()
    test_unsafe_root_containment()
    test_foreign_gate_identity_fails_closed()
    test_gate_success_exception_and_nesting()
    test_gate_validation_transactionality_and_threads()
    test_no_production_imports()
    print(f"\n{COUNT[0] - len(FAILURES)}/{COUNT[0]} checks passed")
    if FAILURES:
        print("FAILURES:")
        for failure in FAILURES:
            print(f"  - {failure}")
        raise SystemExit(1)


if __name__ == "__main__":
    run()
