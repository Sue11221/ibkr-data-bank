"""Mutation reference for the Add Stocks run-manifest lifecycle layer.

Row 85 slice S3 covers the 22 stateful/filesystem functions left after the
pure S2 validator-core slice.  The shipped module is executed from an
in-memory, EOL-normalized source copy.  Every filesystem probe uses a fresh
nested ``<temp-base>/bank`` root and proves cleanup; production storage is
never addressed.
"""

from __future__ import annotations

from collections import Counter
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import copy
import hashlib
import json
import os as real_os
from pathlib import Path
import shutil
import sys
import tempfile
import types
from typing import Callable, Iterator


ENGINE_ROOT = Path(__file__).resolve().parent
TARGET = ENGINE_ROOT / "addstock_run_manifest.py"
EXPECTED_FUNCTION_SEGMENT_SHA256 = {
    "_archive_complete": "f11bbcafa410e2c351514e4c269bebdbd31e5942a126be11cd4ce5e59ee36a9a",
    "_atomic_write": "b84919ccf2af6f270b20f21a296c14a6c68b5f9c6fa9f49de2cf694679da621d",
    "_load_expected": "593e9d82cc9414379b8ed4e4f3ba5bfbd722ae8246698b89f997f5fb9b53f8b3",
    "_maybe_complete": "8e1fa231d598b4e519a560441581faea0de609347392bba1f2db880e84d822e9",
    "_save_mutation": "363ca7a652ebdba31c8fcd2c2d6cf8f60c17a133091c4d959f6506600989028a",
    "_ticker_record": "b01cf01301f49ffdfac9d861fe2a21494ad0f5f9bcaca0b894d41f218a0d1c38",
    "_write_manifest": "93dc584ed4d540f7255add4d233e45aa4549ddb07000ad9d47b753c4e70acbfd",
    "active_path": "af3487b80305ac132c87ee176ffbf267b4a086bce1e44a82b74b294ff1b905c4",
    "archive_complete": "aa133b0fe691b3a3691ea28328cc6699145b20e915e21d04fdb2201cb42f72b6",
    "create_run": "7328b887c2da7ff993cee7c9cdc87eb1ef06ba2141fc5c1409e0fc3dca8b7772",
    "current_xval_intervals": "a0827b17cf4894cfffa77a7299d03f603981f00e21c2e16993f112ce84debbbd",
    "default_archive_dir": "35afdf52c0b6876150051d597825902aac6ca84d4f83b910ac8abe6c5126c481",
    "discard_active": "1664de49f8e57903eedbcc84027075ec7c8caf583f9c3e174adf3f8b437c0333",
    "exempt_empty_verification": "a4726f2fcbc45ec7d1b500ccb75ee0c43694111af06688dae15e46744323e9b3",
    "load_run": "2443174be2aaef9c3186d5b0aad4a7825c461af8e49d2fcfb1351e7aa85672fa",
    "mark_fetch_finished": "7b1f347889d9212aa56ea5c57df143ec8a09d036bc75bdcae09bb3e62b91a7f3",
    "mark_series_complete": "44c9d2daa1b2e7c1bf145a4d47ffa3220cddbb5d78368d9052d0128e7bdae391",
    "mark_series_pending": "968076afe94774cb51c8477376721935ce18908cc51e21bc5d3f9f78dae122be",
    "mark_series_started": "723ad4d9b9bba40f4acd6571b2bdec73f6835560eace9961b31f08ce97737238",
    "mark_verification": "b9fc49879d43507acd85afa128b7cbbad82653cc296a66697f93dcfd56cf28a4",
    "replace_seal_pending": "3a9cf92dfc5767b78ef5d52b59016ed63cac90624d06ef87612ee69347f52ef3",
    "resume_run": "3dee31a4b21823423c28ae55d7656b31f131f8419380f153b60385a40dd12080",
}
FORBIDDEN_TICKERS = frozenset({"APO", "JCI", "OKE", "TKO", "WBD", "TMUS"})
SYNTHETIC_TICKERS = frozenset({
    "AAA", "BBB", "CCC", "EMPTY", "EXT", "PENDING", "PRESENT",
    "RAISE", "RETRY", "SAME", "SEAL", "SYN", "TRUTHY", "VALID",
})

if str(ENGINE_ROOT) not in sys.path:
    sys.path.insert(0, str(ENGINE_ROOT))

from source_segment_custody import (  # noqa: E402
    function_segment_sha256,
    mutation_owner,
    mutation_owners,
    normalize_source_bytes,
)


Probe = Callable[[types.ModuleType], bool]
STAMP = "2026-08-04T12:00:00+00:00"
LATER_STAMP = "2026-08-04T13:00:00+00:00"
NOW = datetime(2026, 8, 4, 12, 0, tzinfo=timezone.utc)
LATER = datetime(2026, 8, 4, 13, 0, tzinfo=timezone.utc)


class CheckKit:
    def __init__(self) -> None:
        self.total = 0
        self.failed = 0

    def section(self, title: str) -> None:
        print(f"=== {title} " + "=" * max(1, 66 - len(title)))

    def check(self, name: str, passed: bool, detail: str = "") -> None:
        self.total += 1
        if passed:
            print(f"[PASS] {name}")
            return
        self.failed += 1
        suffix = f" :: {detail}" if detail else ""
        print(f"[FAIL] {name}{suffix}")

    def finish(self) -> int:
        print()
        print(f"{self.total} checks, {self.failed} failed")
        print("ALL PASS" if not self.failed else "FAILURES PRESENT")
        print(f"harness exit={0 if not self.failed else 3}")
        return 0 if not self.failed else 3


@dataclass(frozen=True)
class Mutation:
    old: str
    new: str

    def apply(self, source: str) -> str:
        count = source.count(self.old)
        if count != 1:
            raise AssertionError(
                f"mutation anchor count={count}, expected 1: {self.old!r}")
        return source.replace(self.old, self.new, 1)


@dataclass(frozen=True)
class Fence:
    fence_id: str
    function: str
    description: str
    mutation: Mutation
    probe: Probe


@dataclass(frozen=True)
class Redundancy:
    clause_id: str
    description: str
    mutation: Mutation
    witness: Probe


def mu(old: str, new: str) -> Mutation:
    return Mutation(old, new)


def f(fence_id: str, function: str, description: str,
      old: str, new: str, probe: Probe) -> Fence:
    return Fence(fence_id, function, description, mu(old, new), probe)


def source_bytes() -> bytes:
    return TARGET.read_bytes()


def digest(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def normalized_source(payload: bytes) -> str:
    return normalize_source_bytes(payload)


def load_module(source: str) -> types.ModuleType:
    module = types.ModuleType("_addstock_lifecycle_fence_subject")
    module.__file__ = str(TARGET)
    module.__package__ = ""
    exec(compile(source, str(TARGET), "exec"), module.__dict__)
    return module


def evaluate(probe: Probe, module: types.ModuleType) -> tuple[bool, str]:
    try:
        result = probe(module)
    except BaseException as exc:  # exact failure behavior is under test
        return False, f"{type(exc).__name__}: {exc}"
    if result is True:
        return True, ""
    return False, f"probe returned {result!r}"


def raises_exact(call: Callable[[], object], error_type: type[BaseException],
                 contains: str | None = None) -> bool:
    try:
        call()
    except BaseException as exc:
        return type(exc) is error_type and (
            contains is None or contains in str(exc))
    return False


def with_attr(module: types.ModuleType, name: str, value: object,
              call: Callable[[], bool]) -> bool:
    previous = getattr(module, name)
    setattr(module, name, value)
    try:
        return call()
    finally:
        setattr(module, name, previous)


_TEMP_BASES: list[Path] = []
_RESIDUE: list[str] = []
_OUTSIDE: list[str] = []


def _inside(base: Path, path: object) -> bool:
    try:
        Path(path).resolve(strict=False).relative_to(base.resolve())
        return True
    except (OSError, TypeError, ValueError):
        return False


def guard_paths(base: Path, *paths: object) -> bool:
    ok = True
    for path in paths:
        if not _inside(base, path):
            _OUTSIDE.append(str(path))
            ok = False
    return ok


@contextmanager
def sandbox(label: str) -> Iterator[tuple[Path, Path]]:
    base = Path(tempfile.mkdtemp(prefix=f"row85_s3_{label}_")).resolve()
    _TEMP_BASES.append(base)
    root = base / "bank"
    root.mkdir(parents=True)
    try:
        yield base, root
        for path in base.rglob("*"):
            guard_paths(base, path)
    finally:
        shutil.rmtree(base, ignore_errors=True)
        if base.exists():
            _RESIDUE.append(str(base))


def series(*, state: str = "pending", xval: bool = False,
           gaps: bool = False) -> dict[str, object]:
    return {"state": state, "xval": xval, "gaps": gaps}


def ticker_record(*,
                  series_map: dict[str, dict[str, object]] | None = None,
                  state: str = "pending", missing: list[str] | None = None,
                  earliest: bool = False, note: str = "",
                  seal_pending: bool = False,
                  updated_at: str = STAMP) -> dict[str, object]:
    return {
        "state": state,
        "updated_at": updated_at,
        "missing": [] if missing is None else list(missing),
        "seal_pending": seal_pending,
        "note": note,
        "earliest": earliest,
        "series": {"1m": series()} if series_map is None else series_map,
    }


def params_for(tickers: dict[str, dict[str, object]]) -> dict[str, object]:
    intervals = sorted({
        interval for record in tickers.values()
        for interval in record["series"]
    })
    kinds = sorted({
        "iv" if "-iv" in interval else
        "hvol" if "-hvol" in interval else
        "bidask" if "-bidask" in interval else "trades"
        for interval in intervals
    })
    return {
        "mode": "add",
        "since": None,
        "intervals": intervals,
        "kinds": kinds,
        "extended": False,
        "store_daily": False,
        "ports_at_start": [],
    }


def manifest(*, tickers: dict[str, dict[str, object]] | None = None,
             run_id: str = "addstock-synthetic", state: str = "active",
             fetch_finished: bool = False,
             finalize: dict[str, object] | None = None) -> dict[str, object]:
    selected = {"AAA": ticker_record()} if tickers is None else tickers
    return {
        "schema": 1,
        "run_id": run_id,
        "created_at": STAMP,
        "updated_at": STAMP,
        "state": state,
        "fetch_finished": fetch_finished,
        "params": params_for(selected),
        "tickers": selected,
        "finalize": ({"at": None, "reason": None, "seal_pending": []}
                     if finalize is None else finalize),
    }


def write_active(module: types.ModuleType, root: Path,
                 data: dict[str, object]) -> bytes:
    raw = module._encode(copy.deepcopy(data))
    path = module.active_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(raw)
    return raw


def create_basic(module: types.ModuleType, root: Path, *,
                 selections: list[tuple[str, str]] | None = None,
                 run_id: object = "addstock-synthetic",
                 earliest: tuple[object, ...] = (),
                 params: dict[str, object] | None = None) -> dict[str, object]:
    return module.create_run(
        root, selections or [("AAA", "1m")], params=params,
        run_id=run_id, now=NOW, earliest_tickers=earliest)


def p(call: Callable[..., bool], *args: object) -> Probe:
    return lambda module: call(module, *args)


class _TraceHandle:
    def __init__(self, handle: object, events: list[str]):
        self._handle = handle
        self._events = events

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self._handle.close()
        return False

    def write(self, payload):
        self._events.append("write")
        return self._handle.write(payload)

    def flush(self):
        self._events.append("flush")
        return self._handle.flush()

    def fileno(self):
        return self._handle.fileno()


class _TraceOS:
    def __init__(self, events: list[str]):
        self.events = events

    def fdopen(self, fd, mode):
        self.events.append(f"fdopen:{mode}")
        return _TraceHandle(real_os.fdopen(fd, mode), self.events)

    def fsync(self, fd):
        self.events.append("fsync")
        return real_os.fsync(fd)

    def replace(self, source, target):
        self.events.append("replace")
        return real_os.replace(source, target)


def probe_active_path(module: types.ModuleType) -> bool:
    with sandbox("active_path") as (base, root):
        result = module.active_path(str(root))
        return (isinstance(result, Path) and result == root / module.ACTIVE_NAME
                and guard_paths(base, result))


def probe_default_archive(module: types.ModuleType) -> bool:
    with sandbox("archive_dir") as (base, root):
        odd_root = root / "child" / ".."
        result = module.default_archive_dir(str(odd_root))
        expected = base / "Run Logs"
        return result == expected and guard_paths(base, result)


def probe_atomic_contract(module: types.ModuleType) -> bool:
    with sandbox("atomic") as (base, root):
        target = root / "nested" / "deeper" / "state.bin"
        events: list[str] = []
        proxy = _TraceOS(events)
        ok = with_attr(
            module, "os", proxy,
            lambda: (module._atomic_write(str(target), b"payload") is None),
        )
        order = [event.split(":", 1)[0] for event in events]
        return (ok and target.read_bytes() == b"payload"
                and all(name in order for name in ("write", "flush", "fsync", "replace"))
                and order.index("write") < order.index("flush")
                < order.index("fsync") < order.index("replace")
                and not list(target.parent.glob(f".{target.name}.*.tmp"))
                and guard_paths(base, target))


def probe_atomic_replace_seam(module: types.ModuleType) -> bool:
    with sandbox("atomic_seam") as (base, root):
        target = root / "state.bin"
        calls: list[tuple[Path, Path]] = []

        def fail(source, destination):
            calls.append((Path(source), Path(destination)))
            raise OSError("injected replace failure")

        failed = raises_exact(
            lambda: module._atomic_write(target, b"payload", replace_fn=fail),
            OSError, "injected replace failure")
        temps = list(root.glob(f".{target.name}.*.tmp"))
        return (failed and len(calls) == 1 and not target.exists() and not temps
                and guard_paths(base, target, calls[0][0], calls[0][1]))


def probe_atomic_same_dir(module: types.ModuleType) -> bool:
    with sandbox("atomic_dir") as (base, root):
        target = root / "state.bin"
        calls: list[object] = []

        def guarded_mkstemp(*args, **kwargs):
            directory = kwargs.get("dir")
            calls.append(directory)
            if Path(directory).resolve() != target.parent.resolve():
                raise AssertionError("temp escaped target directory")
            return tempfile.mkstemp(*args, **kwargs)

        proxy = types.SimpleNamespace(mkstemp=guarded_mkstemp)
        ok = with_attr(
            module, "tempfile", proxy,
            lambda: module._atomic_write(target, b"payload") is None)
        return (ok and calls == [str(target.parent)]
                and target.read_bytes() == b"payload"
                and guard_paths(base, target))


def probe_atomic_missing_tolerated(module: types.ModuleType) -> bool:
    with sandbox("atomic_missing") as (base, root):
        target = root / "state.bin"
        module._atomic_write(target, b"payload")
        return target.read_bytes() == b"payload" and guard_paths(base, target)


def probe_write_manifest(module: types.ModuleType) -> bool:
    with sandbox("write_manifest") as (base, root):
        calls: list[tuple[Path, Path]] = []

        def replace(source, destination):
            calls.append((Path(source), Path(destination)))
            real_os.replace(source, destination)

        data = manifest()
        data["updated_at"] = ""
        result = module._write_manifest(root, data, replace_fn=replace)
        path = module.active_path(root)
        loaded = json.loads(path.read_text(encoding="utf-8"))
        data2 = manifest(run_id="addstock-preserve")
        data2["updated_at"] = LATER_STAMP
        root2 = base / "bank2"
        returned2 = module._write_manifest(root2, data2)
        return (result is data and returned2 is data2
                and data["updated_at"] == STAMP
                and data2["updated_at"] == LATER_STAMP
                and loaded == data and len(calls) == 1
                and calls[0][1] == path
                and guard_paths(base, path, module.active_path(root2)))


class _FaultPath:
    def __init__(self, *, size: int = 8, stat_error: BaseException | None = None,
                 read_error: BaseException | None = None,
                 payload: bytes = b"{}") -> None:
        self._size = size
        self._stat_error = stat_error
        self._read_error = read_error
        self._payload = payload

    def stat(self):
        if self._stat_error is not None:
            raise self._stat_error
        return types.SimpleNamespace(st_size=self._size)

    def read_bytes(self):
        if self._read_error is not None:
            raise self._read_error
        return self._payload


def probe_load_missing(module: types.ModuleType) -> bool:
    with sandbox("load_missing") as (_base, root):
        return module.load_run(root) is None


def probe_load_stat_error(module: types.ModuleType) -> bool:
    fake = _FaultPath(stat_error=OSError("stat boom"))
    return with_attr(module, "active_path", lambda _root: fake, lambda: (
        raises_exact(lambda: module.load_run("unused"), module.ManifestError,
                     "cannot stat active manifest: stat boom")))


def probe_load_low_bound(module: types.ModuleType) -> bool:
    with sandbox("load_low") as (_base, root):
        module.active_path(root).write_bytes(b"")
        return raises_exact(lambda: module.load_run(root), module.ManifestError,
                            "active manifest size is invalid")


def _padded_manifest(module: types.ModuleType, size: int) -> bytes:
    raw = module._encode(manifest())
    if len(raw) > size:
        raise AssertionError("fixture exceeds requested size")
    return raw + b" " * (size - len(raw))


def probe_load_high_bound(module: types.ModuleType) -> bool:
    with sandbox("load_high") as (_base, root):
        path = module.active_path(root)
        path.write_bytes(_padded_manifest(module, module.MAX_BYTES))
        exact = module.load_run(root)
        path.write_bytes(_padded_manifest(module, module.MAX_BYTES + 1))
        over = raises_exact(lambda: module.load_run(root), module.ManifestError,
                            "active manifest size is invalid")
        return exact["run_id"] == "addstock-synthetic" and over


def probe_load_read_oserror(module: types.ModuleType) -> bool:
    fake = _FaultPath(read_error=OSError("read boom"))
    return with_attr(module, "active_path", lambda _root: fake, lambda: (
        raises_exact(lambda: module.load_run("unused"), module.ManifestError,
                     "active manifest is unreadable: read boom")))


def probe_load_unicode(module: types.ModuleType) -> bool:
    fake = _FaultPath(size=2, payload=b"\xff\xfe")
    return with_attr(module, "active_path", lambda _root: fake, lambda: (
        raises_exact(lambda: module.load_run("unused"), module.ManifestError,
                     "active manifest is unreadable")))


def probe_load_value(module: types.ModuleType) -> bool:
    fake = _FaultPath(size=2, payload=b"{]")
    return with_attr(module, "active_path", lambda _root: fake, lambda: (
        raises_exact(lambda: module.load_run("unused"), module.ManifestError,
                     "active manifest is unreadable")))


def probe_load_recursion(module: types.ModuleType) -> bool:
    payload = b"[" * 20000 + b"0" + b"]" * 20000
    fake = _FaultPath(size=len(payload), payload=payload)
    return with_attr(module, "active_path", lambda _root: fake, lambda: (
        raises_exact(lambda: module.load_run("unused"), module.ManifestError,
                     "active manifest is unreadable")))


def probe_load_validation(module: types.ModuleType) -> bool:
    with sandbox("load_validation") as (_base, root):
        bad = manifest()
        bad["state"] = "invented"
        module.active_path(root).write_text(json.dumps(bad), encoding="utf-8")
        return raises_exact(lambda: module.load_run(root), module.ManifestError,
                            "unsupported run state")


def probe_load_unreadable_group(module: types.ModuleType) -> bool:
    return all((
        probe_load_read_oserror(module),
        probe_load_unicode(module),
        probe_load_value(module),
        probe_load_recursion(module),
    ))


def probe_create_canonical(module: types.ModuleType) -> bool:
    with sandbox("create_canon") as (base, root):
        data = module.create_run(
            root, [(" aaa ", " 1m "), ("AAA", "1d"), ("BBB", "1m-pre")],
            run_id="addstock-canon", now=NOW)
        invalid = raises_exact(
            lambda: module.create_run(
                base / "bank2", [("AAA", "not-an-interval")],
                run_id="addstock-bad-selection", now=NOW),
            module.ManifestError, "invalid interval")
        return (set(data["tickers"]) == {"AAA", "BBB"}
                and set(data["tickers"]["AAA"]["series"]) == {"1m", "1d"}
                and invalid
                and guard_paths(base, module.active_path(root)))


def probe_create_duplicate_witness(module: types.ModuleType) -> bool:
    with sandbox("create_dup") as (base, root):
        left = module.create_run(
            root, [("AAA", "1m"), ("AAA", "1m"), ("AAA", "1d")],
            run_id="addstock-dup", now=NOW)
        root2 = base / "bank2"
        right = module.create_run(
            root2, [("AAA", "1m"), ("AAA", "1d")],
            run_id="addstock-dup", now=NOW)
        return (left == right
                and module.active_path(root).read_bytes()
                == module.active_path(root2).read_bytes())


def probe_create_empty(module: types.ModuleType) -> bool:
    with sandbox("create_empty") as (base, root):
        return (raises_exact(
            lambda: module.create_run(root, [], run_id="addstock-empty", now=NOW),
            module.ManifestError, "a run needs at least one selection")
                and raises_exact(
                    lambda: module.create_run(
                        base / "bank2", None,
                        run_id="addstock-empty-none", now=NOW),
                    module.ManifestError, "a run needs at least one selection"))


def probe_create_ticker_cap(module: types.ModuleType) -> bool:
    with sandbox("create_ticker_cap") as (_base, root):
        old_cap = module.MAX_TICKERS
        old_write = module._write_manifest
        calls: list[object] = []
        module.MAX_TICKERS = 3
        module._write_manifest = lambda _root, data, replace_fn=None: (
            calls.append(data) or data)
        try:
            failed = raises_exact(
                lambda: module.create_run(
                    root, [(f"S{index}", "1m") for index in range(4)],
                    run_id="addstock-cap", now=NOW),
                module.ManifestError, "too many tickers")
        finally:
            module.MAX_TICKERS = old_cap
            module._write_manifest = old_write
        return failed and not calls


def probe_create_series_cap(module: types.ModuleType) -> bool:
    with sandbox("create_series_cap") as (_base, root):
        selections = [("AAA", f"{index}m")
                      for index in range(1, module.MAX_SERIES_PER_TICKER + 2)]
        return raises_exact(
            lambda: module.create_run(
                root, selections, run_id="addstock-series-cap", now=NOW),
            module.ManifestError, "too many series for one ticker")


def probe_create_earliest(module: types.ModuleType) -> bool:
    with sandbox("create_earliest") as (base, root):
        data = module.create_run(
            root, [("AAA", "1m"), ("BBB", "1m")],
            run_id="addstock-earliest", now=NOW,
            earliest_tickers=(" aaa ", "!!!", object()))
        none_data = module.create_run(
            base / "bank2", [("AAA", "1m")],
            run_id="addstock-earliest-none", now=NOW,
            earliest_tickers=None)
        return (data["tickers"]["AAA"]["earliest"] is True
                and data["tickers"]["BBB"]["earliest"] is False
                and none_data["tickers"]["AAA"]["earliest"] is False)


def probe_create_stamp(module: types.ModuleType) -> bool:
    with sandbox("create_stamp") as (_base, root):
        data = module.create_run(
            root, [("AAA", "1m")], run_id="addstock-stamp", now=LATER)
        return (data["created_at"] == LATER_STAMP
                and data["updated_at"] == LATER_STAMP
                and data["tickers"]["AAA"]["updated_at"] == LATER_STAMP)


def probe_create_run_id(module: types.ModuleType) -> bool:
    with sandbox("create_runid") as (base, root):
        old = module.new_run_id
        module.new_run_id = lambda: "addstock-generated"
        try:
            first = module.create_run(root, [("AAA", "1m")], now=NOW)
            root2 = base / "bank2"
            second = module.create_run(
                root2, [("AAA", "1m")], run_id="", now=NOW)
        finally:
            module.new_run_id = old
        return (first["run_id"] == "addstock-generated"
                and second["run_id"] == "addstock-generated")


def probe_create_run_id_validation(module: types.ModuleType) -> bool:
    with sandbox("create_runid_bad") as (_base, root):
        old_write = module._write_manifest
        calls: list[object] = []
        module._write_manifest = lambda _root, data, replace_fn=None: (
            calls.append(data) or data)
        try:
            failed = raises_exact(
                lambda: module.create_run(
                    root, [("AAA", "1m")], run_id="bad id!", now=NOW),
                module.ManifestError, "run_id has unsafe characters")
        finally:
            module._write_manifest = old_write
        return failed and not calls


def probe_create_run_id_coercion(module: types.ModuleType) -> bool:
    with sandbox("create_runid_int") as (_base, root):
        data = module.create_run(root, [("AAA", "1m")], run_id=123, now=NOW)
        return data["run_id"] == "123"


def probe_create_params(module: types.ModuleType) -> bool:
    with sandbox("create_params") as (_base, root):
        data = module.create_run(
            root, [("AAA", "1m"), ("AAA", "1d")],
            params={"mode": "add", "intervals": ["1d", "1m"],
                    "kinds": ["trades"], "ports_at_start": [2000, "3000"]},
            run_id="addstock-params", now=NOW)
        return (data["params"]["intervals"] == ["1d", "1m"]
                and data["params"]["ports_at_start"] == [2000, 3000])


def probe_create_shape(module: types.ModuleType) -> bool:
    with sandbox("create_shape") as (_base, root):
        data = module.create_run(
            root, [("AAA", "1m"), ("BBB", "1m-pre")],
            run_id="addstock-shape", now=NOW, earliest_tickers=["AAA"])
        return (data["schema"] == module.SCHEMA
                and data["state"] == "active"
                and data["fetch_finished"] is False
                and data["finalize"] == {
                    "at": None, "reason": None, "seal_pending": []}
                and data["tickers"]["AAA"]["earliest"] is True
                and data["tickers"]["BBB"]["series"]["1m-pre"]
                == {"state": "pending", "xval": True, "gaps": True})


def probe_create_existing(module: types.ModuleType, invalid: bool = False) -> bool:
    with sandbox("create_existing") as (_base, root):
        if invalid:
            before = b"{broken"
            module.active_path(root).write_bytes(before)
            expected = "existing manifest is invalid"
        else:
            first = create_basic(module, root, run_id="addstock-first")
            before = module.active_path(root).read_bytes()
            expected = f"run {first['run_id']} is {first['state']}"
        try:
            module.create_run(
                root, [("BBB", "1m")], run_id="addstock-second", now=NOW)
        except BaseException as exc:
            return (type(exc) is module.ActiveRunExists
                    and expected in str(exc)
                    and module.active_path(root).read_bytes() == before)
        return False


def probe_create_replace(module: types.ModuleType) -> bool:
    with sandbox("create_replace") as (_base, root):
        calls = []

        def fail(source, destination):
            calls.append((source, destination))
            raise OSError("create replace")

        return (raises_exact(
            lambda: module.create_run(
                root, [("AAA", "1m")], run_id="addstock-replace",
                now=NOW, replace_fn=fail), OSError, "create replace")
                and len(calls) == 1 and not module.active_path(root).exists())


def probe_load_expected(module: types.ModuleType, mode: str) -> bool:
    if mode == "missing":
        old = module.load_run
        module.load_run = lambda _root: None
        try:
            return raises_exact(
                lambda: module._load_expected("unused", "run"),
                module.ManifestError, "active Add Stocks manifest is missing")
        finally:
            module.load_run = old
    sentinel = {"run_id": "123"}
    old = module.load_run
    module.load_run = lambda _root: sentinel
    try:
        if mode == "match":
            return module._load_expected("unused", 123) is sentinel
        try:
            module._load_expected("unused", "stale")
        except BaseException as exc:
            return (type(exc) is module.RunMismatch
                    and "stale" in str(exc) and "123" in str(exc))
        return False
    finally:
        module.load_run = old


def probe_ticker_record(module: types.ModuleType, mode: str) -> bool:
    data = {"tickers": {"AAA": {"value": 1}}}
    if mode == "match":
        ticker, record = module._ticker_record(data, " aaa ")
        return ticker == "AAA" and record is data["tickers"]["AAA"]
    if mode == "invalid":
        return raises_exact(
            lambda: module._ticker_record(data, "!!!"), module.ManifestError,
            "invalid ticker")
    return raises_exact(
        lambda: module._ticker_record(data, "BBB"), module.ManifestError,
        "ticker is not part of run: BBB")


def probe_save_mutation(module: types.ModuleType, mode: str) -> bool:
    with sandbox("save_mutation") as (_base, root):
        data = manifest()
        if mode == "stamp":
            result = module._save_mutation(root, data, LATER_STAMP)
            return (result is data and data["updated_at"] == LATER_STAMP
                    and module.load_run(root)["updated_at"] == LATER_STAMP)
        calls = []

        def fail(source, destination):
            calls.append((source, destination))
            raise OSError("save replace")

        return (raises_exact(
            lambda: module._save_mutation(
                root, data, LATER_STAMP, replace_fn=fail),
            OSError, "save replace") and len(calls) == 1)


def _prepare_mark_root(module: types.ModuleType, label: str,
                       selections: list[tuple[str, str]]) \
        -> tuple[object, Path, Path, dict[str, object]]:
    manager = sandbox(label)
    base, root = manager.__enter__()
    data = create_basic(
        module, root, selections=selections, run_id="addstock-mark",
        earliest=("AAA", "BBB"))
    return manager, base, root, data


def probe_mark_started(module: types.ModuleType, mode: str) -> bool:
    manager, _base, root, data = _prepare_mark_root(
        module, "mark_started", [("AAA", "1m"), ("AAA", "1m-pre")])
    try:
        if mode == "missing":
            return raises_exact(
                lambda: module.mark_series_started(
                    root, data["run_id"], "AAA", "2m", now=LATER),
                module.ManifestError, "series is not part of run")
        current = module.load_run(root)
        current["tickers"]["AAA"]["note"] = "old"
        current["tickers"]["AAA"]["series"]["1m"]["xval"] = True
        current["tickers"]["AAA"]["series"]["1m"]["gaps"] = True
        write_active(module, root, current)
        interval = " 1m " if mode != "extended" else "1m-pre"
        result = module.mark_series_started(
            root, data["run_id"], " aaa ", interval, now=LATER)
        loaded = module.load_run(root)
        record = loaded["tickers"]["AAA"]
        selected = record["series"][interval.strip()]
        if mode == "extended":
            return (selected == {"state": "building", "xval": True,
                                 "gaps": True})
        return (result == loaded and selected == {
                    "state": "building", "xval": False, "gaps": False}
                and record["state"] == "building"
                and record["updated_at"] == LATER_STAMP
                and record["note"] == "")
    finally:
        manager.__exit__(None, None, None)


def probe_mark_pending(module: types.ModuleType, mode: str) -> bool:
    manager, _base, root, data = _prepare_mark_root(
        module, "mark_pending", [("AAA", "1m")])
    try:
        if mode == "missing":
            return raises_exact(
                lambda: module.mark_series_pending(
                    root, data["run_id"], "AAA", "2m", now=LATER),
                module.ManifestError, "series is not part of run")
        module.mark_series_started(root, data["run_id"], "AAA", "1m", now=NOW)
        note = "  needs    retry  " + "x" * 500
        result = module.mark_series_pending(
            root, data["run_id"], " aaa ", " 1m ", note, now=LATER)
        loaded = module.load_run(root)
        record = loaded["tickers"]["AAA"]
        return (result == loaded
                and record["series"]["1m"]["state"] == "pending"
                and record["state"] == "pending"
                and record["updated_at"] == LATER_STAMP
                and record["note"] == " ".join(note.split())[:module.MAX_NOTE])
    finally:
        manager.__exit__(None, None, None)


def probe_mark_complete(module: types.ModuleType, mode: str) -> bool:
    manager, _base, root, data = _prepare_mark_root(
        module, "mark_complete", [("AAA", "1m"), ("AAA", "1m-pre")])
    try:
        if mode == "type":
            before = module.active_path(root).read_bytes()
            failed = raises_exact(
                lambda: module.mark_series_complete(
                    root, data["run_id"], "AAA", "1m", empty=1, now=LATER),
                module.ManifestError, "empty must be a boolean")
            return failed and module.active_path(root).read_bytes() == before
        if mode == "missing":
            return raises_exact(
                lambda: module.mark_series_complete(
                    root, data["run_id"], "AAA", "2m", now=LATER),
                module.ManifestError, "series is not part of run")
        result = module.mark_series_complete(
            root, data["run_id"], " aaa ", " 1m ",
            empty=(mode == "empty"), now=LATER)
        loaded = module.load_run(root)
        record = loaded["tickers"]["AAA"]
        selected = record["series"]["1m"]
        expected_flags = mode == "empty"
        return (result == loaded and selected["state"] == "complete"
                and selected["xval"] is expected_flags
                and selected["gaps"] is expected_flags
                and record["updated_at"] == LATER_STAMP
                and record["note"] == ""
                and record["state"] == "building")
    finally:
        manager.__exit__(None, None, None)


class _UnlinkFailure:
    def __init__(self, path: Path):
        self.path = path

    def unlink(self):
        raise OSError("unlink boom")


def _complete_data() -> dict[str, object]:
    rec = ticker_record(
        series_map={"1m-pre": series(state="complete", xval=True, gaps=True)},
        state="verified", earliest=True)
    return manifest(
        tickers={"AAA": rec}, state="complete", fetch_finished=True,
        finalize={"at": STAMP, "reason": "complete", "seal_pending": []})


def probe_archive(module: types.ModuleType, mode: str) -> bool:
    with sandbox("archive") as (base, root):
        data = _complete_data()
        raw = write_active(module, root, data)
        archive = base / "custom-logs"
        target = archive / f"addstock-run-{data['run_id']}.json"
        if mode == "read_error":
            archive.mkdir(parents=True)
            target.mkdir()
            failed = raises_exact(
                lambda: module._archive_complete(root, data, archive_dir=archive),
                module.ManifestError, "cannot read existing archive")
            return failed and module.active_path(root).read_bytes() == raw
        if mode == "same":
            archive.mkdir(parents=True)
            target.write_bytes(raw)
            old = module._atomic_write
            module._atomic_write = lambda *_a, **_k: (_ for _ in ()).throw(
                AssertionError("identical archive was rewritten"))
            try:
                result = module._archive_complete(root, data, archive_dir=archive)
            finally:
                module._atomic_write = old
            return result == target and not module.active_path(root).exists()
        if mode == "different":
            archive.mkdir(parents=True)
            target.write_bytes(b"different")
            failed = raises_exact(
                lambda: module._archive_complete(root, data, archive_dir=archive),
                module.ManifestError, "archive already exists with different bytes")
            return (failed and module.active_path(root).read_bytes() == raw
                    and target.read_bytes() == b"different")
        if mode == "unlink":
            real_active = module.active_path(root)
            old = module.active_path
            module.active_path = lambda _root: _UnlinkFailure(real_active)
            try:
                failed = raises_exact(
                    lambda: module._archive_complete(
                        root, data, archive_dir=archive),
                    module.ManifestError, "could not remove active manifest: unlink boom")
            finally:
                module.active_path = old
            return failed and real_active.exists() and target.read_bytes() == raw
        calls = []

        def replace(source, destination):
            calls.append((Path(source), Path(destination)))
            real_os.replace(source, destination)

        result = module._archive_complete(
            root, data, archive_dir=(None if mode == "default" else archive),
            replace_fn=replace)
        expected = (module.default_archive_dir(root) if mode == "default"
                    else archive) / target.name
        return (result == expected and expected.read_bytes() == raw
                and not module.active_path(root).exists() and len(calls) == 1
                and guard_paths(base, expected, calls[0][0], calls[0][1]))


def probe_archive_complete(module: types.ModuleType, mode: str) -> bool:
    with sandbox("archive_public") as (base, root):
        if mode == "state":
            data = manifest()
            write_active(module, root, data)
            return raises_exact(
                lambda: module.archive_complete(root, data["run_id"], base / "logs"),
                module.ManifestError, "only a complete run can be archived")
        data = _complete_data()
        write_active(module, root, data)
        if mode == "stale":
            return raises_exact(
                lambda: module.archive_complete(
                    root, "addstock-stale", base / "logs"),
                module.RunMismatch, "active run is 'addstock-synthetic'")
        target = module.archive_complete(root, data["run_id"], base / "logs")
        return (type(target) is str and Path(target).is_file()
                and not module.active_path(root).exists()
                and guard_paths(base, target))


def _maybe_data() -> dict[str, object]:
    rec = ticker_record(
        series_map={"1m": series(state="complete", xval=True, gaps=True)},
        state="verified", earliest=True)
    return manifest(tickers={"AAA": rec}, fetch_finished=True)


def probe_maybe(module: types.ModuleType, mode: str) -> bool:
    data = _maybe_data()
    if mode == "fetch":
        data["fetch_finished"] = False
    elif mode == "finalize":
        data["finalize"]["seal_pending"] = ["AAA 1m"]
    elif mode == "ticker_seal":
        data["tickers"]["AAA"]["seal_pending"] = True
    elif mode == "verified":
        data["tickers"]["AAA"]["state"] = "built"
    calls: list[object] = []

    def save(_root, value, stamp, replace_fn=None):
        calls.append(("save", stamp, replace_fn))
        return value

    def write(_root, value, replace_fn=None):
        calls.append(("write", value["state"], replace_fn))
        return value

    def archive(_root, value, archive_dir=None, replace_fn=None):
        calls.append(("archive", value["state"], archive_dir, replace_fn))
        return Path("synthetic-archive.json")

    old_save, old_write, old_archive = (
        module._save_mutation, module._write_manifest, module._archive_complete)
    module._save_mutation, module._write_manifest, module._archive_complete = (
        save, write, archive)
    try:
        result = module._maybe_complete(
            "root", data, LATER_STAMP, archive_dir="archive-dir")
    finally:
        module._save_mutation, module._write_manifest, module._archive_complete = (
            old_save, old_write, old_archive)
    if mode in {"fetch", "finalize", "ticker_seal", "verified", "blocked"}:
        return (result == {"manifest": data, "archived": None}
                and [item[0] for item in calls] == ["save"]
                and data["state"] == "active")
    return (data["state"] == "complete"
            and data["finalize"]["at"] == LATER_STAMP
            and data["finalize"]["reason"] == "complete"
            and data["updated_at"] == LATER_STAMP
            and [item[0] for item in calls] == ["write", "archive"]
            and calls[1][2] == "archive-dir"
            and result == {"manifest": data,
                           "archived": "synthetic-archive.json"})


def probe_exempt(module: types.ModuleType, mode: str) -> bool:
    if mode == "callable":
        with sandbox("exempt_callable") as (_base, root):
            data = create_basic(module, root)
            return raises_exact(
                lambda: module.exempt_empty_verification(
                    root, data["run_id"], None), module.ManifestError,
                "empty_fn must be callable")
    if mode == "stamp_once":
        with sandbox("exempt_stamp") as (_base, root):
            run = create_basic(
                module, root,
                selections=[("AAA", "1m"), ("BBB", "1d-hvol")],
                run_id="addstock-exempt-stamp", earliest=("AAA", "BBB"))
            module.mark_series_complete(
                root, run["run_id"], "AAA", "1m", now=NOW)
            module.mark_series_complete(
                root, run["run_id"], "BBB", "1d-hvol", now=NOW)
            clock_calls: list[int] = []

            def clock():
                clock_calls.append(1)
                return LATER

            result = module.exempt_empty_verification(
                root, run["run_id"], lambda *_args: True, now=clock)
            return (result["exempted"] == [("AAA", "1m"), ("BBB", "1d-hvol")]
                    and len(clock_calls) == 1)
    if mode == "rth_only":
        rec = ticker_record(
            series_map={"1m-pre": series(
                state="complete", xval=False, gaps=False)}, state="built")
        data = manifest(tickers={"EXT": rec}, run_id="addstock-rth-only")
        calls: list[tuple[str, str]] = []
        old = module._load_expected
        module._load_expected = lambda _root, _run_id: data
        try:
            result = module.exempt_empty_verification(
                "unused", data["run_id"],
                lambda ticker, interval: calls.append((ticker, interval)) or False,
                now=LATER)
        finally:
            module._load_expected = old
        return result == {"exempted": [], "archived": None} and calls == []
    with sandbox("exempt") as (base, root):
        selections = [
            ("EMPTY", "1m"), ("PRESENT", "1d-hvol"),
            ("RAISE", "1m-iv"), ("TRUTHY", "1m"),
            ("PENDING", "1d-hvol"), ("EXT", "1m-pre"),
            ("SAME", "1d"),
        ]
        run = create_basic(
            module, root, selections=selections, run_id="addstock-exempt",
            earliest=tuple(ticker for ticker, _iv in selections))
        for ticker, interval in selections:
            if ticker != "PENDING":
                module.mark_series_complete(
                    root, run["run_id"], ticker, interval,
                    empty=(ticker == "SAME"), now=NOW)
        calls: list[tuple[str, str]] = []

        def classify(ticker, interval):
            calls.append((ticker, interval))
            if ticker == "RAISE":
                raise OSError("predicate boom")
            if ticker == "TRUTHY":
                return 1
            return ticker == "EMPTY"

        archive = base / "logs"
        result = module.exempt_empty_verification(
            root, run["run_id"], classify, now=LATER,
            archive_dir=archive)
        loaded = module.load_run(root)
        before_noop = module.active_path(root).read_bytes()
        noop = module.exempt_empty_verification(
            root, run["run_id"], lambda *_args: False,
            now=lambda: (_ for _ in ()).throw(
                AssertionError("no-op read the clock")), archive_dir=archive)
        after_noop = module.active_path(root).read_bytes()
        expected_calls = [
            ("EMPTY", "1m"), ("PRESENT", "1d-hvol"),
            ("RAISE", "1m-iv"), ("TRUTHY", "1m"),
        ]
        return (result == {"exempted": [("EMPTY", "1m")],
                           "archived": None}
                and calls == expected_calls
                and loaded["tickers"]["EMPTY"]["series"]["1m"]["xval"]
                and loaded["tickers"]["EMPTY"]["series"]["1m"]["gaps"]
                and loaded["tickers"]["EMPTY"]["state"] == "verified"
                and loaded["tickers"]["EMPTY"]["updated_at"] == LATER_STAMP
                and loaded["tickers"]["PRESENT"]["state"] == "built"
                and loaded["tickers"]["RAISE"]["state"] == "built"
                and loaded["tickers"]["TRUTHY"]["state"] == "built"
                and loaded["tickers"]["PENDING"]["state"] == "pending"
                and noop == {"exempted": [], "archived": None}
                and before_noop == after_noop)


def probe_mark_verification(module: types.ModuleType, mode: str) -> bool:
    if mode == "rth_only":
        rec = ticker_record(
            series_map={"1m-pre": series(
                state="complete", xval=False, gaps=False)}, state="built")
        data = manifest(tickers={"AAA": rec}, run_id="addstock-verify-rth")
        old_load, old_maybe = module._load_expected, module._maybe_complete
        module._load_expected = lambda _root, _run_id: data
        module._maybe_complete = lambda _root, value, _stamp, archive_dir=None: {
            "manifest": value, "archived": None}
        try:
            module.mark_verification(
                "unused", data["run_id"], "AAA", "xval", now=LATER)
        finally:
            module._load_expected, module._maybe_complete = old_load, old_maybe
        return data["tickers"]["AAA"]["series"]["1m-pre"]["xval"] is False
    with sandbox("mark_verify") as (base, root):
        selections = [("AAA", "1m"), ("AAA", "1d"),
                      ("AAA", "1m-pre"), ("BBB", "1m")]
        run = create_basic(
            module, root, selections=selections, run_id="addstock-verify")
        for ticker, interval in selections:
            module.mark_series_complete(root, run["run_id"], ticker, interval, now=NOW)
        archive = base / "logs"
        if mode == "bad_stage":
            return raises_exact(
                lambda: module.mark_verification(
                    root, run["run_id"], "AAA", "invented", now=LATER),
                module.ManifestError, "unsupported verification stage")
        if mode == "no_match":
            return raises_exact(
                lambda: module.mark_verification(
                    root, run["run_id"], "AAA", "xval", ["2m"], now=LATER),
                module.ManifestError, "no requested RTH series matched")
        if mode == "earliest":
            result = module.mark_verification(
                root, run["run_id"], "AAA", " EARLIEST ",
                intervals=["not-an-interval"], now=LATER, archive_dir=archive)
            loaded = module.load_run(root)
            return (result["archived"] is None
                    and loaded["tickers"]["AAA"]["earliest"] is True)
        intervals = None if mode == "all" else [" 1m "]
        result = module.mark_verification(
            root, run["run_id"], " aaa ", " XVAL ", intervals,
            now=LATER, archive_dir=archive)
        loaded = module.load_run(root)
        aaa = loaded["tickers"]["AAA"]
        if mode == "all":
            expected = aaa["series"]["1m"]["xval"] \
                and aaa["series"]["1d"]["xval"]
        else:
            expected = (aaa["series"]["1m"]["xval"]
                        and not aaa["series"]["1d"]["xval"])
        return (result["archived"] is None and expected
                and aaa["series"]["1m-pre"]["xval"] is True
                and aaa["updated_at"] == LATER_STAMP)


def _seal_items(module: types.ModuleType) -> list[str]:
    return [" AAA   1m ", "", "AAA 1m", "BBB " + "x" * 200]


def probe_mark_fetch(module: types.ModuleType, mode: str) -> bool:
    with sandbox("mark_fetch") as (base, root):
        run = create_basic(
            module, root, selections=[("AAA", "1m"), ("BBB", "1m")],
            run_id="addstock-fetch")
        if mode == "cap":
            values = [f"S{index} 1m" for index in range(module.MAX_SEAL_PENDING + 1)]
            return raises_exact(
                lambda: module.mark_fetch_finished(
                    root, run["run_id"], seal_pending=values, now=LATER),
                module.ManifestError, "too many seal-pending entries")
        result = module.mark_fetch_finished(
            root, run["run_id"], reason="  user    pause  " + "z" * 100,
            seal_pending=_seal_items(module), now=LATER,
            archive_dir=base / "logs")
        loaded = module.load_run(root)
        expected_seals = ["AAA 1m", ("BBB " + "x" * 200)[:128]]
        return (result["archived"] is None
                and loaded["fetch_finished"] is True
                and loaded["state"] == "interrupted"
                and loaded["finalize"] == {
                    "at": LATER_STAMP,
                    "reason": ("user pause " + "z" * 100)[:80],
                    "seal_pending": expected_seals}
                and loaded["tickers"]["AAA"]["seal_pending"] is True
                and loaded["tickers"]["BBB"]["seal_pending"] is True)


def probe_replace_seals(module: types.ModuleType, mode: str) -> bool:
    with sandbox("replace_seals") as (base, root):
        run = create_basic(
            module, root, selections=[("AAA", "1m"), ("BBB", "1m")],
            run_id="addstock-replace-seals")
        module.mark_fetch_finished(
            root, run["run_id"], seal_pending=["AAA 1m"], now=NOW,
            archive_dir=base / "logs")
        if mode == "cap":
            values = [f"S{index} 1m" for index in range(module.MAX_SEAL_PENDING + 1)]
            return raises_exact(
                lambda: module.replace_seal_pending(
                    root, run["run_id"], values, now=LATER),
                module.ManifestError, "too many seal-pending entries")
        result = module.replace_seal_pending(
            root, run["run_id"], [
                " BBB   1m ", "", "BBB 1m", "CCC " + "x" * 200],
            now=LATER, archive_dir=base / "logs")
        loaded = module.load_run(root)
        return (result["archived"] is None
                and loaded["finalize"]["seal_pending"] == [
                    "BBB 1m", ("CCC " + "x" * 200)[:128]]
                and loaded["tickers"]["AAA"]["seal_pending"] is False
                and loaded["tickers"]["BBB"]["seal_pending"] is True
                and all(record["updated_at"] == LATER_STAMP
                        for record in loaded["tickers"].values()))


def probe_resume(module: types.ModuleType, mode: str) -> bool:
    with sandbox("resume") as (base, root):
        if mode == "complete":
            write_active(module, root, _complete_data())
            return raises_exact(
                lambda: module.resume_run(root, "addstock-synthetic", now=LATER),
                module.ManifestError, "a complete run cannot be resumed")
        run = create_basic(
            module, root, selections=[("AAA", "1m"), ("AAA", "1m-pre")],
            run_id="addstock-resume")
        module.mark_series_started(root, run["run_id"], "AAA", "1m", now=NOW)
        module.mark_series_complete(root, run["run_id"], "AAA", "1m-pre", now=NOW)
        module.mark_fetch_finished(
            root, run["run_id"], reason="pause", seal_pending=["AAA 1m"],
            now=NOW, archive_dir=base / "logs")
        before = module.load_run(root)
        result = module.resume_run(root, run["run_id"], now=LATER)
        loaded = module.load_run(root)
        return (result == loaded and loaded["state"] == "active"
                and loaded["fetch_finished"] is False
                and loaded["finalize"]["at"] is None
                and loaded["finalize"]["reason"] is None
                and loaded["finalize"]["seal_pending"]
                == before["finalize"]["seal_pending"]
                and loaded["tickers"]["AAA"]["seal_pending"] is True
                and loaded["tickers"]["AAA"]["series"]["1m"]["state"]
                == "pending"
                and loaded["tickers"]["AAA"]["series"]["1m-pre"]["state"]
                == "complete"
                and loaded["tickers"]["AAA"]["updated_at"] == LATER_STAMP)


def _xval_entry(**updates: object) -> dict[str, object]:
    entry: dict[str, object] = {
        "ticker": "AAA",
        "interval": "1m",
        "interval_fingerprint": {"current": True, "after_sha256": "a" * 64},
        "reference_interval": "1d-iv",
        "reference_interval_fingerprint": {
            "current": True, "after_sha256": "b" * 64},
    }
    entry.update(updates)
    return entry


class _DictLikeFingerprint:
    """Non-dict with dict-like values; proves strict type gates are load-bearing."""

    def __init__(self, values: dict[str, object]) -> None:
        self._values = values

    def get(self, key: str, default: object = None) -> object:
        return self._values.get(key, default)


def probe_current_xval(module: types.ModuleType, mode: str) -> bool:
    calls: list[tuple[str, str]] = []

    def selected(_root, ticker, interval):
        calls.append((ticker, interval))
        if mode == "selected_error":
            raise OSError("selected boom")
        if mode == "observed_shape":
            return _DictLikeFingerprint({"sha256": "a" * 64})
        if mode == "selected_sha":
            return {"sha256": "bad"}
        return {"sha256": ("c" * 64 if mode == "selected_mismatch" else "a" * 64)}

    def optional(_root, ticker, interval):
        calls.append((ticker, interval))
        if mode == "reference_error":
            raise OSError("reference boom")
        if mode == "reference_sha":
            return {"sha256": "bad"}
        return {"sha256": ("c" * 64 if mode == "reference_mismatch" else "b" * 64)}

    entry: object = _xval_entry()
    if mode == "nondict":
        entry = "bad"
    elif mode == "canon":
        entry = _xval_entry(ticker="!!!")
    elif mode == "selected_shape":
        entry = _xval_entry(interval_fingerprint=_DictLikeFingerprint({
            "current": True, "after_sha256": "a" * 64}))
    elif mode == "selected_current":
        entry = _xval_entry(interval_fingerprint={
            "current": 1, "after_sha256": "a" * 64})
    elif mode == "selected_sha":
        entry = _xval_entry(interval_fingerprint={
            "current": True, "after_sha256": "bad"})
    elif mode == "reference_shape":
        entry = _xval_entry(reference_interval_fingerprint=_DictLikeFingerprint({
            "current": True, "after_sha256": "b" * 64}))
    elif mode == "reference_current":
        entry = _xval_entry(reference_interval_fingerprint={
            "current": 1, "after_sha256": "b" * 64})
    elif mode == "reference_sha":
        entry = _xval_entry(reference_interval_fingerprint={
            "current": True, "after_sha256": "bad"})
    elif mode == "reference_interval":
        entry = _xval_entry(reference_interval="bad interval")
    elif mode == "no_reference":
        entry = _xval_entry(reference_interval_fingerprint=None)
    entries = [entry, entry] if mode == "dedupe" else [entry]
    result = module.current_xval_intervals(
        "synthetic-root", entries, fingerprint_fn=selected,
        optional_fingerprint_fn=optional)
    invalid_modes = {
        "nondict", "canon", "selected_shape", "selected_current",
        "selected_sha", "selected_mismatch", "selected_error", "observed_shape",
        "reference_shape", "reference_current", "reference_sha",
        "reference_interval", "reference_mismatch", "reference_error",
    }
    if mode in invalid_modes:
        return result == []
    expected_calls = [("AAA", "1m")] if mode == "no_reference" else [
        ("AAA", "1m"), ("AAA", "1d-iv")]
    if mode == "dedupe":
        expected_calls *= 2
    return result == ["1m"] and calls == expected_calls


def probe_discard(module: types.ModuleType, mode: str) -> bool:
    with sandbox("discard") as (base, root):
        archive = base / "logs"
        if mode == "missing":
            return module.discard_active(root, archive, now=LATER) is None
        if mode == "invalid":
            raw = b"{broken"
            module.active_path(root).write_bytes(raw)
            target_text = module.discard_active(root, archive, now=LATER)
            target = Path(target_text)
            return ("-invalid-discarded-" in target.name
                    and target.read_bytes() == raw
                    and not module.active_path(root).exists())
        data = create_basic(module, root, run_id="addstock-discard")
        raw = module.active_path(root).read_bytes()
        if mode == "collision":
            archive.mkdir(parents=True)
            stamp = LATER_STAMP.replace(":", "").replace("+", "-")
            first = archive / (
                f"addstock-run-{data['run_id']}-discarded-{stamp}.json")
            first.write_bytes(b"existing")
        if mode == "replace_error":
            old = module.os
            proxy = types.SimpleNamespace(**{
                name: getattr(real_os, name) for name in dir(real_os)
                if not name.startswith("__")})
            proxy.replace = lambda *_args: (_ for _ in ()).throw(
                OSError("discard boom"))
            module.os = proxy
            try:
                failed = raises_exact(
                    lambda: module.discard_active(root, archive, now=LATER),
                    module.ManifestError,
                    "could not discard active manifest: discard boom")
            finally:
                module.os = old
            return failed and module.active_path(root).read_bytes() == raw
        target_text = module.discard_active(
            root, None if mode == "default" else archive, now=LATER)
        target = Path(target_text)
        return (type(target_text) is str and target.read_bytes() == raw
                and not module.active_path(root).exists()
                and ("-1.json" in target.name if mode == "collision" else True)
                and data["run_id"] in target.name
                and (target.parent == module.default_archive_dir(root)
                     if mode == "default" else target.parent == archive)
                and "2026-08-04T130000-0000" in target.name
                and guard_paths(base, target))


def fences() -> tuple[Fence, ...]:
    cases = [
        f("AP1", "active_path", "coerces a string root to Path",
          "return Path(root) / ACTIVE_NAME", "return root / ACTIVE_NAME",
          probe_active_path),
        f("AP2", "active_path", "uses the canonical active filename",
          "return Path(root) / ACTIVE_NAME",
          "return Path(root) / 'wrong-active.json'", probe_active_path),

        f("DA1", "default_archive_dir", "coerces a string root to Path",
          'return Path(root).resolve().parent / "Run Logs"',
          'return root.resolve().parent / "Run Logs"', probe_default_archive),
        f("DA2", "default_archive_dir", "resolves dot segments before parenting",
          'return Path(root).resolve().parent / "Run Logs"',
          'return Path(root).parent / "Run Logs"', probe_default_archive),
        f("DA3", "default_archive_dir", "places archives in sibling Run Logs",
          'return Path(root).resolve().parent / "Run Logs"',
          'return Path(root).resolve() / "Run Logs"', probe_default_archive),

        f("AW1", "_atomic_write", "creates all missing parent directories",
          "path.parent.mkdir(parents=True, exist_ok=True)", "pass",
          probe_atomic_contract),
        f("AW2", "_atomic_write", "creates temp in target directory",
          'suffix=".tmp", dir=str(path.parent))',
          'suffix=".tmp", dir=None)', probe_atomic_same_dir),
        f("AW3", "_atomic_write", "opens temp in binary write mode",
          'with os.fdopen(fd, "wb") as handle:',
          'with os.fdopen(fd, "w") as handle:', probe_atomic_contract),
        f("AW4", "_atomic_write", "writes the supplied bytes",
          "handle.write(raw)", "handle.write(b'')", probe_atomic_contract),
        f("AW5", "_atomic_write", "flushes before replace",
          "handle.flush()", "None", probe_atomic_contract),
        f("AW6", "_atomic_write", "fsyncs before replace",
          "os.fsync(handle.fileno())", "None", probe_atomic_contract),
        f("AW7", "_atomic_write", "honors injected replace seam",
          "(replace_fn or os.replace)(temp, path)", "os.replace(temp, path)",
          probe_atomic_replace_seam),
        f("AW8", "_atomic_write", "cleans temp after replace failure",
          "temp.unlink()", "None", probe_atomic_replace_seam),
        f("AW9", "_atomic_write", "tolerates temp already moved away",
          "except FileNotFoundError:\n            pass",
          "except PermissionError:\n            pass", probe_atomic_missing_tolerated),

        f("WM1", "_write_manifest", "defaults a blank update stamp to creation",
          'data["updated_at"] = data.get("updated_at") or data["created_at"]',
          'data["updated_at"] = data.get("updated_at")', probe_write_manifest),
        f("WM2", "_write_manifest", "forwards the replace seam",
          "_atomic_write(active_path(root), _encode(data), replace_fn=replace_fn)",
          "_atomic_write(active_path(root), _encode(data))", probe_write_manifest),
        f("WM3", "_write_manifest", "returns the mutated manifest object",
          "    return data\n\n\ndef load_run(root):",
          "    return None\n\n\ndef load_run(root):", probe_write_manifest),

        f("LR1", "load_run", "missing active file returns None",
          "except FileNotFoundError:\n        return None",
          "except FileNotFoundError:\n        raise", probe_load_missing),
        f("LR2", "load_run", "stat errors are normalized",
          'raise ManifestError(f"cannot stat active manifest: {exc}") from exc',
          'raise OSError(f"cannot stat active manifest: {exc}") from exc',
          probe_load_stat_error),
        f("LR3", "load_run", "rejects zero-byte manifests",
          "if size <= 0 or size > MAX_BYTES:",
          "if size < 0 or size > MAX_BYTES:", probe_load_low_bound),
        f("LR4", "load_run", "accepts exactly MAX_BYTES",
          "if size <= 0 or size > MAX_BYTES:",
          "if size <= 0 or size >= MAX_BYTES:", probe_load_high_bound),
        f("LR5", "load_run", "rejects MAX_BYTES plus one",
          "if size <= 0 or size > MAX_BYTES:",
          "if size <= 0 or size > MAX_BYTES + 1:", probe_load_high_bound),
        f("LR6", "load_run", "reads bytes from the active path",
          "raw = path.read_bytes()", "raw = b'{}'", probe_load_read_oserror),
        f("LR7", "load_run", "parses the file JSON",
          'data = json.loads(raw.decode("utf-8"))', "data = {}",
          probe_load_value),
        f("LR8", "load_run", "all read and parse failures normalize",
          "except (OSError, UnicodeError, ValueError, RecursionError) as exc:",
          "except () as exc:", probe_load_unreadable_group),
        f("LR9", "load_run", "validates parseable manifests",
          "return validate_manifest(data)\n\n\ndef create_run",
          "return data\n\n\ndef create_run", probe_load_validation),

        f("CR1", "create_run", "None selections normalize to empty",
          "for ticker, interval in selections or []:",
          "for ticker, interval in selections:", probe_create_empty),
        f("CR2", "create_run", "canonicalizes every selection",
          "item = _canon_selection(ticker, interval)",
          "item = (ticker, interval)", probe_create_canonical),
        f("CR3", "create_run", "requires at least one selection",
          "if not normalized:\n        raise ManifestError(\"a run needs at least one selection\")",
          "if False:\n        raise ManifestError(\"a run needs at least one selection\")",
          probe_create_empty),
        f("CR4", "create_run", "enforces the unique ticker cap early",
          "if len({ticker for ticker, _iv in normalized}) > MAX_TICKERS:",
          "if False:", probe_create_ticker_cap),
        f("CR5", "create_run", "groups all intervals per ticker",
          "per_ticker.setdefault(ticker, {})[interval] = _series_record(interval)",
          "per_ticker[ticker] = {interval: _series_record(interval)}",
          probe_create_canonical),
        f("CR6", "create_run", "enforces the per-ticker series cap",
          "if any(len(series) > MAX_SERIES_PER_TICKER for series in per_ticker.values()):",
          "if False:", probe_create_series_cap),
        f("CR7", "create_run", "None earliest sidecar normalizes to empty",
          "for ticker in earliest_tickers or ():",
          "for ticker in earliest_tickers:", probe_create_earliest),
        f("CR8", "create_run", "canonicalizes earliest ticker identities",
          "earliest.add(ss.canonical_ticker(str(ticker)))",
          "earliest.add(str(ticker))", probe_create_earliest),
        f("CR9", "create_run", "ignores irrelevant earliest-sidecar junk",
          "except Exception:  # noqa: BLE001 - irrelevant sidecar junk is ignored\n            continue",
          "except Exception:  # noqa: BLE001 - irrelevant sidecar junk is ignored\n            raise",
          probe_create_earliest),
        f("CR10", "create_run", "uses the injected creation clock",
          "stamp = _utc_text(now)\n    run_id = run_id or new_run_id()",
          "stamp = _utc_text(None)\n    run_id = run_id or new_run_id()",
          probe_create_stamp),
        f("CR11", "create_run", "generates an id for missing or blank input",
          "run_id = run_id or new_run_id()", "run_id = run_id",
          probe_create_run_id),
        f("CR12", "create_run", "rejects unsafe run ids",
          "if not _RUN_ID_RE.fullmatch(str(run_id)):", "if False:",
          probe_create_run_id_validation),
        f("CR13", "create_run", "coerces run ids before regex validation",
          "_RUN_ID_RE.fullmatch(str(run_id))", "_RUN_ID_RE.fullmatch(run_id)",
          probe_create_run_id_coercion),
        f("CR14", "create_run", "stores the canonical string run id",
          '"run_id": str(run_id),', '"run_id": run_id,',
          probe_create_run_id_coercion),
        f("CR15", "create_run", "normalizes run parameters",
          '"params": _normalize_params(params, normalized),',
          '"params": params,', probe_create_params),
        f("CR16", "create_run", "starts with fetching unfinished",
          '"fetch_finished": False,\n        "params": _normalize_params',
          '"fetch_finished": True,\n        "params": _normalize_params',
          probe_create_shape),
        f("CR17", "create_run", "records canonical earliest membership",
          '"earliest": ticker in earliest,', '"earliest": False,',
          probe_create_earliest),
        f("CR18", "create_run", "refuses to overwrite an active manifest",
          "if active_path(root).exists():", "if False:",
          lambda m: (probe_create_existing(m) and probe_create_existing(m, True))),
        f("CR19", "create_run", "normalizes an invalid existing manifest",
          "except ManifestError as exc:\n                detail = f\"existing manifest is invalid ({exc})\"",
          "except ActiveRunExists as exc:\n                detail = f\"existing manifest is invalid ({exc})\"",
          p(probe_create_existing, True)),
        f("CR20", "create_run", "raises the dedicated exclusivity error",
          "raise ActiveRunExists(f\"active Add Stocks state exists: {detail}\")",
          "raise ManifestError(f\"active Add Stocks state exists: {detail}\")",
          probe_create_existing),
        f("CR21", "create_run", "forwards replace failure without residue",
          "return _write_manifest(root, data, replace_fn=replace_fn)\n\n\ndef _load_expected",
          "return _write_manifest(root, data)\n\n\ndef _load_expected",
          probe_create_replace),
    ]
    cases += [
        f("LE1", "_load_expected", "rejects a missing active run",
          "if data is None:\n        raise ManifestError(\"active Add Stocks manifest is missing\")",
          "if False:\n        raise ManifestError(\"active Add Stocks manifest is missing\")",
          p(probe_load_expected, "missing")),
        f("LE2", "_load_expected", "coerces the expected run id to string",
          'if data["run_id"] != str(run_id):',
          'if data["run_id"] != run_id:', p(probe_load_expected, "match")),
        f("LE3", "_load_expected", "raises RunMismatch naming both ids",
          "raise RunMismatch(\n            f\"stale run {run_id!r}; active run is {data['run_id']!r}\")",
          "raise ManifestError(\n            f\"stale run {run_id!r}; active run is {data['run_id']!r}\")",
          p(probe_load_expected, "mismatch")),
        f("LE4", "_load_expected", "returns the matched manifest",
          "    return data\n\n\ndef _ticker_record",
          "    return None\n\n\ndef _ticker_record", p(probe_load_expected, "match")),

        f("TR1", "_ticker_record", "canonicalizes lookup tickers",
          "def _ticker_record(data, ticker):\n    try:\n        ticker = ss.canonical_ticker(str(ticker))",
          "def _ticker_record(data, ticker):\n    try:\n        ticker = str(ticker).strip()",
          p(probe_ticker_record, "match")),
        f("TR2", "_ticker_record", "normalizes canonicalization failures",
          "except Exception as exc:  # noqa: BLE001\n        raise ManifestError(f\"invalid ticker: {ticker!r}\") from exc",
          "except () as exc:  # noqa: BLE001\n        raise ManifestError(f\"invalid ticker: {ticker!r}\") from exc",
          p(probe_ticker_record, "invalid")),
        f("TR3", "_ticker_record", "rejects a ticker outside the run",
          'return ticker, data["tickers"][ticker]',
          'return ticker, data["tickers"].get(ticker)',
          p(probe_ticker_record, "missing")),

        f("SM1", "_save_mutation", "sets the supplied update stamp",
          'data["updated_at"] = stamp\n    return _write_manifest',
          'data["updated_at"] = data["updated_at"]\n    return _write_manifest',
          p(probe_save_mutation, "stamp")),
        f("SM2", "_save_mutation", "forwards its replace seam",
          "return _write_manifest(root, data, replace_fn=replace_fn)\n\n\ndef mark_series_started",
          "return _write_manifest(root, data)\n\n\ndef mark_series_started",
          p(probe_save_mutation, "replace")),

        f("ST1", "mark_series_started", "canonicalizes ticker and interval",
          "def mark_series_started(root, run_id, ticker, interval, *, now=None):\n    ticker, interval = _canon_selection(ticker, interval)",
          "def mark_series_started(root, run_id, ticker, interval, *, now=None):\n    ticker, interval = ticker, interval",
          p(probe_mark_started, "normal")),
        f("ST2", "mark_series_started", "rejects an absent series",
          'if interval not in record["series"]:\n            raise ManifestError(f"series is not part of run: {ticker} {interval}")\n        stamp = _utc_text(now)\n        series = record["series"][interval]\n        series["state"] = "building"',
          'if False:\n            raise ManifestError(f"series is not part of run: {ticker} {interval}")\n        stamp = _utc_text(now)\n        series = record["series"][interval]\n        series["state"] = "building"',
          p(probe_mark_started, "missing")),
        f("ST3", "mark_series_started", "moves the selected series to building",
          'series["state"] = "building"\n        if _is_rth(interval):',
          'series["state"] = "pending"\n        if _is_rth(interval):',
          p(probe_mark_started, "normal")),
        f("ST4", "mark_series_started", "distinguishes RTH from extended series",
          "if _is_rth(interval):\n            series[\"xval\"] = False\n            series[\"gaps\"] = False",
          "if False:\n            series[\"xval\"] = False\n            series[\"gaps\"] = False",
          p(probe_mark_started, "normal")),
        f("ST5", "mark_series_started", "reopens both RTH verification debts",
          'series["xval"] = False\n            series["gaps"] = False',
          'series["xval"] = True\n            series["gaps"] = True',
          p(probe_mark_started, "normal")),
        f("ST6", "mark_series_started", "stamps the ticker record",
          'record["updated_at"] = stamp\n        record["note"] = ""\n        _recompute_ticker(record)\n        return _save_mutation(root, data, stamp)\n\n\ndef mark_series_pending',
          'record["updated_at"] = record["updated_at"]\n        record["note"] = ""\n        _recompute_ticker(record)\n        return _save_mutation(root, data, stamp)\n\n\ndef mark_series_pending',
          p(probe_mark_started, "normal")),
        f("ST7", "mark_series_started", "clears note and recomputes ticker state",
          'record["note"] = ""\n        _recompute_ticker(record)\n        return _save_mutation(root, data, stamp)\n\n\ndef mark_series_pending',
          'record["note"] = record["note"]\n        None\n        return _save_mutation(root, data, stamp)\n\n\ndef mark_series_pending',
          p(probe_mark_started, "normal")),
        f("ST8", "mark_series_started", "persists the mutation",
          "return _save_mutation(root, data, stamp)\n\n\ndef mark_series_pending",
          "return data\n\n\ndef mark_series_pending",
          p(probe_mark_started, "normal")),

        f("PD1", "mark_series_pending", "rejects an absent series",
          'record["series"]:\n            raise ManifestError(f"series is not part of run: {ticker} {interval}")\n        stamp = _utc_text(now)\n        record["series"][interval]["state"] = "pending"',
          'record["series"] and False:\n            raise ManifestError(f"series is not part of run: {ticker} {interval}")\n        stamp = _utc_text(now)\n        record["series"][interval]["state"] = "pending"',
          p(probe_mark_pending, "missing")),
        f("PD2", "mark_series_pending", "returns the series to pending",
          'record["series"][interval]["state"] = "pending"',
          'record["series"][interval]["state"] = "building"',
          p(probe_mark_pending, "normal")),
        f("PD3", "mark_series_pending", "normalizes and bounds the retry note",
          'record["note"] = " ".join(str(note).split())[:MAX_NOTE]',
          'record["note"] = str(note)[:MAX_NOTE]', p(probe_mark_pending, "normal")),
        f("PD4", "mark_series_pending", "stamps the ticker record",
          'record["updated_at"] = stamp\n        record["note"] = " ".join(str(note).split())[:MAX_NOTE]',
          'record["updated_at"] = record["updated_at"]\n        record["note"] = " ".join(str(note).split())[:MAX_NOTE]',
          p(probe_mark_pending, "normal")),
        f("PD5", "mark_series_pending", "recomputes and persists ticker state",
          'record["note"] = " ".join(str(note).split())[:MAX_NOTE]\n        _recompute_ticker(record)\n        return _save_mutation(root, data, stamp)\n\n\ndef mark_series_complete',
          'record["note"] = " ".join(str(note).split())[:MAX_NOTE]\n        return data\n        return _save_mutation(root, data, stamp)\n\n\ndef mark_series_complete',
          p(probe_mark_pending, "normal")),

        f("CP1", "mark_series_complete", "requires a real bool empty flag",
          "if not isinstance(empty, bool):", "if empty not in (True, False):",
          p(probe_mark_complete, "type")),
        f("CP2", "mark_series_complete", "rejects an absent series",
          'if interval not in record["series"]:\n            raise ManifestError(f"series is not part of run: {ticker} {interval}")\n        stamp = _utc_text(now)\n        series = record["series"][interval]\n        series["state"] = "complete"',
          'if False:\n            raise ManifestError(f"series is not part of run: {ticker} {interval}")\n        stamp = _utc_text(now)\n        series = record["series"][interval]\n        series["state"] = "complete"',
          p(probe_mark_complete, "missing")),
        f("CP3", "mark_series_complete", "moves the selected series to complete",
          'series["state"] = "complete"\n        if empty:',
          'series["state"] = "building"\n        if empty:',
          p(probe_mark_complete, "normal")),
        f("CP4", "mark_series_complete", "applies waivers only for empty series",
          "if empty:\n            series[\"xval\"] = True\n            series[\"gaps\"] = True",
          "if False:\n            series[\"xval\"] = True\n            series[\"gaps\"] = True",
          p(probe_mark_complete, "empty")),
        f("CP5", "mark_series_complete", "waives both verification debts when empty",
          'series["xval"] = True\n            series["gaps"] = True\n        record["updated_at"]',
          'series["xval"] = False\n            series["gaps"] = False\n        record["updated_at"]',
          p(probe_mark_complete, "empty")),
        f("CP6", "mark_series_complete", "clears note and recomputes ticker state",
          'record["note"] = ""\n        _recompute_ticker(record)\n        return _save_mutation(root, data, stamp)\n\n\ndef _archive_complete',
          'record["note"] = record["note"]\n        None\n        return _save_mutation(root, data, stamp)\n\n\ndef _archive_complete',
          p(probe_mark_complete, "normal")),
        f("CP7", "mark_series_complete", "persists the completion",
          "return _save_mutation(root, data, stamp)\n\n\ndef _archive_complete",
          "return data\n\n\ndef _archive_complete",
          p(probe_mark_complete, "normal")),

        f("AR1", "_archive_complete", "uses the contained default archive",
          "def _archive_complete(root, data, archive_dir=None, replace_fn=None):\n    archive_dir = Path(archive_dir or default_archive_dir(root))",
          "def _archive_complete(root, data, archive_dir=None, replace_fn=None):\n    archive_dir = Path(archive_dir)",
          p(probe_archive, "default")),
        f("AR3", "_archive_complete", "names archive by run identity",
          'target = archive_dir / f"addstock-run-{data[\'run_id\']}.json"',
          'target = archive_dir / "addstock-run-wrong.json"',
          p(probe_archive, "new")),
        f("AR4", "_archive_complete", "never overwrites an existing archive",
          "if target.exists():\n        try:\n            existing = target.read_bytes()",
          "if False:\n        try:\n            existing = target.read_bytes()",
          p(probe_archive, "different")),
        f("AR5", "_archive_complete", "normalizes existing-archive read errors",
          'raise ManifestError(f"cannot read existing archive: {exc}") from exc',
          'raise OSError(f"cannot read existing archive: {exc}") from exc',
          p(probe_archive, "read_error")),
        f("AR6", "_archive_complete", "requires byte-identical retry archives",
          "if existing != raw:\n            raise ManifestError(",
          "if False:\n            raise ManifestError(", p(probe_archive, "different")),
        f("AR7", "_archive_complete", "forwards the archive replace seam",
          "_atomic_write(target, raw, replace_fn=replace_fn)",
          "_atomic_write(target, raw)", p(probe_archive, "new")),
        f("AR8", "_archive_complete", "removes active only after archive custody",
          "active_path(root).unlink()\n    except OSError as exc:",
          "None\n    except OSError as exc:", p(probe_archive, "new")),
        f("AR9", "_archive_complete", "normalizes active unlink failures",
          'raise ManifestError(f"could not remove active manifest: {exc}") from exc',
          'raise OSError(f"could not remove active manifest: {exc}") from exc',
          p(probe_archive, "unlink")),

        f("PA1", "archive_complete", "checks the requested run identity",
          "data = _load_expected(root, run_id)\n        if data[\"state\"] != \"complete\":",
          "data = load_run(root)\n        if data[\"state\"] != \"complete\":",
          p(probe_archive_complete, "stale")),
        f("PA2", "archive_complete", "archives complete runs only",
          'if data["state"] != "complete":', "if False:",
          p(probe_archive_complete, "state")),
        f("PA3", "archive_complete", "returns a serializable path string",
          "return str(_archive_complete(root, data, archive_dir=archive_dir))",
          "return _archive_complete(root, data, archive_dir=archive_dir)",
          p(probe_archive_complete, "normal")),

        f("MB1", "_maybe_complete", "waits for fetch completion",
          'if (not data["fetch_finished"]\n            or data["finalize"].get("seal_pending")',
          'if (False\n            or data["finalize"].get("seal_pending")',
          p(probe_maybe, "fetch")),
        f("MB2", "_maybe_complete", "waits for finalize seal debt",
          'or data["finalize"].get("seal_pending")\n            or any(record.get("seal_pending")',
          'or False\n            or any(record.get("seal_pending")',
          p(probe_maybe, "finalize")),
        f("MB3", "_maybe_complete", "waits for per-ticker seal debt",
          'any(record.get("seal_pending")\n                   for record in data["tickers"].values())',
          "False", p(probe_maybe, "ticker_seal")),
        f("MB4", "_maybe_complete", "waits for every ticker verification",
          'any(\n                record["state"] != "verified"\n                for record in data["tickers"].values())',
          "False", p(probe_maybe, "verified")),
        f("MB5", "_maybe_complete", "persists blocked progress without archive",
          'return {"manifest": _save_mutation(root, data, stamp),\n                "archived": None}',
          'return {"manifest": data,\n                "archived": None}', p(probe_maybe, "fetch")),
        f("MB6", "_maybe_complete", "writes complete state and finalize metadata",
          'data["state"] = "complete"\n    data["finalize"]["at"] = stamp\n    data["finalize"]["reason"] = "complete"\n    data["updated_at"] = stamp',
          'data["state"] = "active"\n    data["finalize"]["at"] = None\n    data["finalize"]["reason"] = None\n    data["updated_at"] = data["updated_at"]',
          p(probe_maybe, "ready")),
        f("MB7", "_maybe_complete", "writes complete manifest before archive",
          "_write_manifest(root, data)\n    target = _archive_complete",
          "None\n    target = _archive_complete", p(probe_maybe, "ready")),
        f("MB8", "_maybe_complete", "returns the exact archive path",
          "target = _archive_complete(root, data, archive_dir=archive_dir)\n    return {\"manifest\": data, \"archived\": str(target)}",
          "target = None\n    return {\"manifest\": data, \"archived\": str(target)}",
          p(probe_maybe, "ready")),
    ]
    cases += [
        f("EX1", "exempt_empty_verification", "requires a callable predicate",
          "if not callable(empty_fn):", "if False:", p(probe_exempt, "callable")),
        f("EX2", "exempt_empty_verification", "considers RTH series only",
          'if (not _is_rth(interval)\n                        or series["state"] != "complete"',
          'if (False\n                        or series["state"] != "complete"',
          p(probe_exempt, "rth_only")),
        f("EX3", "exempt_empty_verification", "considers complete series only",
          'or series["state"] != "complete"\n                        or (series["xval"] and series["gaps"]))',
          'or False\n                        or (series["xval"] and series["gaps"]))',
          p(probe_exempt, "flow")),
        f("EX4", "exempt_empty_verification", "skips already discharged series",
          'or (series["xval"] and series["gaps"])):',
          'or False):', p(probe_exempt, "flow")),
        f("EX5", "exempt_empty_verification", "predicate exceptions retain debt",
          "except Exception:  # noqa: BLE001 - retain uncertain debt\n                    continue",
          "except Exception:  # noqa: BLE001 - retain uncertain debt\n                    raise",
          p(probe_exempt, "flow")),
        f("EX6", "exempt_empty_verification", "accepts only literal True",
          "if is_empty is not True:", "if not is_empty:", p(probe_exempt, "flow")),
        f("EX7", "exempt_empty_verification", "reads the injected clock lazily once",
          "if stamp is None:\n                    stamp = _utc_text(now)",
          "if True:\n                    stamp = _utc_text(now)",
          p(probe_exempt, "stamp_once")),
        f("EX8", "exempt_empty_verification", "waives both verification debts",
          'series["xval"] = True\n                series["gaps"] = True',
          'series["xval"] = False\n                series["gaps"] = False',
          p(probe_exempt, "flow")),
        f("EX9", "exempt_empty_verification", "records and recomputes each exemption",
          "exempted.append((ticker, interval))\n                changed = True",
          "None\n                changed = False", p(probe_exempt, "flow")),
        f("EX10", "exempt_empty_verification", "zero exemptions perform no write",
          "if not exempted:\n            return {\"exempted\": [], \"archived\": None}",
          "if False:\n            return {\"exempted\": [], \"archived\": None}",
          p(probe_exempt, "flow")),
        f("EX11", "exempt_empty_verification", "passes accepted waivers to completion logic",
          "result = _maybe_complete(\n            root, data, stamp, archive_dir=archive_dir)",
          "result = {\"archived\": \"wrong-archive\"}", p(probe_exempt, "flow")),

        f("VF1", "mark_verification", "normalizes the stage vocabulary",
          'stage = str(stage or "").strip().lower()',
          'stage = str(stage or "")', p(probe_mark_verification, "earliest")),
        f("VF2", "mark_verification", "rejects unsupported stages",
          "if stage not in DEBT_STAGES:", "if False:",
          p(probe_mark_verification, "bad_stage")),
        f("VF3", "mark_verification", "earliest ignores interval filters",
          'if stage == "earliest":\n            record["earliest"] = True',
          'if False:\n            record["earliest"] = True',
          p(probe_mark_verification, "earliest")),
        f("VF4", "mark_verification", "None intervals selects every RTH series",
          "if intervals is not None:\n                chosen =",
          "if True:\n                chosen =", p(probe_mark_verification, "all")),
        f("VF5", "mark_verification", "canonicalizes chosen intervals",
          "chosen = {_canon_selection(ticker, iv)[1] for iv in intervals}",
          "chosen = set(intervals)", p(probe_mark_verification, "selected")),
        f("VF6", "mark_verification", "never marks an extended-hours series",
          'if not _is_rth(interval) or (chosen is not None\n                                             and interval not in chosen):',
          'if False or (chosen is not None\n                                             and interval not in chosen):',
          p(probe_mark_verification, "rth_only")),
        f("VF7", "mark_verification", "respects a nonempty chosen set",
          'or (chosen is not None\n                                             and interval not in chosen):',
          'or False:', p(probe_mark_verification, "selected")),
        f("VF8", "mark_verification", "marks the requested debt stage",
          "series[stage] = True\n                matched = True",
          "series[stage] = False\n                matched = True",
          p(probe_mark_verification, "selected")),
        f("VF9", "mark_verification", "records that an RTH series matched",
          "series[stage] = True\n                matched = True",
          "series[stage] = True\n                matched = False",
          p(probe_mark_verification, "selected")),
        f("VF10", "mark_verification", "rejects a chosen set with no RTH match",
          "if chosen is not None and not matched:\n                raise ManifestError(",
          "if False:\n                raise ManifestError(",
          p(probe_mark_verification, "no_match")),
    ]

    fetch_prefix = (
        'def mark_fetch_finished(root, run_id, reason="complete", seal_pending=(), *,\n'
        '                        now=None, archive_dir=None):\n'
        '    reason = " ".join(str(reason or "complete").split())[:80]\n'
        '    seal_pending = list(dict.fromkeys(\n'
        '        " ".join(str(item).split())[:128] for item in seal_pending or ()\n'
        '        if str(item).strip()))'
    )
    replace_prefix = (
        'def replace_seal_pending(root, run_id, seal_pending=(), *, now=None,\n'
        '                         archive_dir=None):\n'
        '    """Replace seal debt after the existing sealer and a fresh gap scan."""\n'
        '    seal_pending = list(dict.fromkeys(\n'
        '        " ".join(str(item).split())[:128] for item in seal_pending or ()\n'
        '        if str(item).strip()))'
    )
    cases += [
        f("FF1", "mark_fetch_finished", "normalizes and bounds the finish reason",
          fetch_prefix,
          fetch_prefix.replace(
              'reason = " ".join(str(reason or "complete").split())[:80]',
              'reason = str(reason)'), p(probe_mark_fetch, "flow")),
        f("FF2", "mark_fetch_finished", "collapses seal-item whitespace",
          fetch_prefix,
          fetch_prefix.replace('" ".join(str(item).split())', 'str(item)'),
          p(probe_mark_fetch, "flow")),
        f("FF3", "mark_fetch_finished", "bounds each seal item to 128 chars",
          fetch_prefix, fetch_prefix.replace('[:128] for item', ' for item'),
          p(probe_mark_fetch, "flow")),
        f("FF4", "mark_fetch_finished", "drops blank seal items",
          fetch_prefix,
          fetch_prefix.replace('if str(item).strip()))', 'if True))'),
          p(probe_mark_fetch, "flow")),
        f("FF5", "mark_fetch_finished", "deduplicates seals in first-seen order",
          fetch_prefix,
          fetch_prefix.replace('list(dict.fromkeys(\n', 'list(\n').replace(
              'if str(item).strip()))', 'if str(item).strip())'),
          p(probe_mark_fetch, "flow")),
        f("FF6", "mark_fetch_finished", "enforces the seal list cap",
          fetch_prefix + '\n    if len(seal_pending) > MAX_SEAL_PENDING:',
          fetch_prefix + '\n    if False:', p(probe_mark_fetch, "cap")),
        f("FF7", "mark_fetch_finished", "records terminal fetch interruption state",
          'data["fetch_finished"] = True\n        data["state"] = "interrupted"\n        data["finalize"] = {',
          'data["fetch_finished"] = False\n        data["state"] = "active"\n        data["finalize"] = {',
          p(probe_mark_fetch, "flow")),
        f("FF8", "mark_fetch_finished", "stores the normalized finalize seal list",
          '"seal_pending": seal_pending,\n        }\n        pending_tickers',
          '"seal_pending": [],\n        }\n        pending_tickers',
          p(probe_mark_fetch, "flow")),
        f("FF9", "mark_fetch_finished", "derives ticker flags from first seal token",
          "pending_tickers = {item.split()[0] for item in seal_pending}\n        for ticker, record in data[\"tickers\"].items():",
          "pending_tickers = set(seal_pending)\n        for ticker, record in data[\"tickers\"].items():",
          p(probe_mark_fetch, "flow")),
        f("FF10", "mark_fetch_finished", "recomputes and runs completion logic",
          "return _maybe_complete(root, data, stamp, archive_dir=archive_dir)\n\n\ndef replace_seal_pending",
          "return data\n\n\ndef replace_seal_pending",
          p(probe_mark_fetch, "flow")),

        f("RP1", "replace_seal_pending", "collapses replacement-item whitespace",
          replace_prefix,
          replace_prefix.replace('" ".join(str(item).split())', 'str(item)'),
          p(probe_replace_seals, "flow")),
        f("RP2", "replace_seal_pending", "bounds replacement items to 128 chars",
          replace_prefix, replace_prefix.replace('[:128] for item', ' for item'),
          p(probe_replace_seals, "flow")),
        f("RP3", "replace_seal_pending", "drops blank replacement items",
          replace_prefix,
          replace_prefix.replace('if str(item).strip()))', 'if True))'),
          p(probe_replace_seals, "flow")),
        f("RP4", "replace_seal_pending", "deduplicates replacements in order",
          replace_prefix,
          replace_prefix.replace('list(dict.fromkeys(\n', 'list(\n').replace(
              'if str(item).strip()))', 'if str(item).strip())'),
          p(probe_replace_seals, "flow")),
        f("RP5", "replace_seal_pending", "enforces replacement seal cap",
          replace_prefix + '\n    if len(seal_pending) > MAX_SEAL_PENDING:',
          replace_prefix + '\n    if False:', p(probe_replace_seals, "cap")),
        f("RP6", "replace_seal_pending", "derives flags from first seal token",
          "stamp = _utc_text(now)\n        pending_tickers = {item.split()[0] for item in seal_pending}",
          "stamp = _utc_text(now)\n        pending_tickers = set(seal_pending)",
          p(probe_replace_seals, "flow")),
        f("RP7", "replace_seal_pending", "replaces the finalize seal list",
          'data["finalize"]["seal_pending"] = seal_pending',
          'data["finalize"]["seal_pending"] = []',
          p(probe_replace_seals, "flow")),
        f("RP8", "replace_seal_pending", "updates every ticker seal flag and stamp",
          'record["seal_pending"] = ticker in pending_tickers\n            record["updated_at"] = stamp\n            _recompute_ticker(record)',
          'record["seal_pending"] = False\n            record["updated_at"] = record["updated_at"]\n            _recompute_ticker(record)',
          p(probe_replace_seals, "flow")),
        f("RP9", "replace_seal_pending", "runs completion after replacing seals",
          "return _maybe_complete(\n            root, data, stamp, archive_dir=archive_dir)\n\n\ndef resume_run",
          "return data\n\n\ndef resume_run",
          p(probe_replace_seals, "flow")),

        f("RS1", "resume_run", "refuses a complete run",
          'if data["state"] == "complete":', "if False:",
          p(probe_resume, "complete")),
        f("RS2", "resume_run", "returns run state to active",
          'data["state"] = "active"\n        data["fetch_finished"] = False',
          'data["state"] = "interrupted"\n        data["fetch_finished"] = False',
          p(probe_resume, "flow")),
        f("RS3", "resume_run", "reopens fetch completion",
          'data["fetch_finished"] = False\n        data["finalize"]["at"] = None',
          'data["fetch_finished"] = True\n        data["finalize"]["at"] = None',
          p(probe_resume, "flow")),
        f("RS4", "resume_run", "clears finalize time and reason",
          'data["finalize"]["at"] = None\n        data["finalize"]["reason"] = None',
          'data["finalize"]["at"] = data["finalize"]["at"]\n        data["finalize"]["reason"] = data["finalize"]["reason"]',
          p(probe_resume, "flow")),
        f("RS5", "resume_run", "preserves outstanding seal debt",
          'data["finalize"]["reason"] = None\n        for record in data["tickers"].values():',
          'data["finalize"]["reason"] = None\n        data["finalize"]["seal_pending"] = []\n        for record in data["tickers"].values():',
          p(probe_resume, "flow")),
        f("RS6", "resume_run", "demotes building series only",
          'if series["state"] == "building":',
          'if series["state"] in {"building", "complete"}:',
          p(probe_resume, "flow")),
        f("RS7", "resume_run", "demotes building state to pending",
          'series["state"] = "pending"\n            record["updated_at"] = stamp',
          'series["state"] = "building"\n            record["updated_at"] = stamp',
          p(probe_resume, "flow")),
        f("RS8", "resume_run", "stamps and recomputes every ticker",
          'record["updated_at"] = stamp\n            _recompute_ticker(record)\n        return _save_mutation(root, data, stamp)',
          'record["updated_at"] = record["updated_at"]\n            None\n        return data',
          p(probe_resume, "flow")),

        f("XV1", "current_xval_intervals", "skips non-dict evidence entries",
          "if not isinstance(entry, dict):\n            continue",
          "if False:\n            continue", p(probe_current_xval, "nondict")),
        f("XV2", "current_xval_intervals", "skips selection canonicalization failures",
          "except ManifestError:\n            continue\n        selected = entry.get(\"interval_fingerprint\")",
          "except ():\n            continue\n        selected = entry.get(\"interval_fingerprint\")",
          p(probe_current_xval, "canon")),
        f("XV3", "current_xval_intervals", "requires a selected fingerprint object",
          "not isinstance(selected, dict)\n                or selected.get(\"current\") is not True",
          "False\n                or selected.get(\"current\") is not True",
          p(probe_current_xval, "selected_shape")),
        f("XV4", "current_xval_intervals", "requires literal current True",
          'selected.get("current") is not True',
          'selected.get("current") != True', p(probe_current_xval, "selected_current")),
        f("XV5", "current_xval_intervals", "requires selected SHA-256 syntax",
          'or not _valid_sha256(selected.get("after_sha256"))):',
          'or False):', p(probe_current_xval, "selected_sha")),
        f("XV6", "current_xval_intervals", "uses the injected selected fingerprint",
          "observed = fingerprint_fn(root, ticker, interval)",
          "observed = optional_fingerprint_fn(root, ticker, interval)",
          p(probe_current_xval, "valid")),
        f("XV7", "current_xval_intervals", "requires an observed fingerprint dict",
          "not isinstance(observed, dict)\n                    or observed.get(\"sha256\")",
          "False\n                    or observed.get(\"sha256\")",
          p(probe_current_xval, "observed_shape")),
        f("XV8", "current_xval_intervals", "requires selected fingerprint equality",
          'observed.get("sha256")\n                    != selected.get("after_sha256")',
          'observed.get("sha256")\n                    == selected.get("after_sha256")',
          p(probe_current_xval, "selected_mismatch")),
        f("XV9", "current_xval_intervals", "reference evidence is optional",
          "if reference is not None:", "if True:",
          p(probe_current_xval, "no_reference")),
        f("XV10", "current_xval_intervals", "requires a reference fingerprint dict",
          "not isinstance(reference, dict)\n                        or reference.get(\"current\") is not True",
          "False\n                        or reference.get(\"current\") is not True",
          p(probe_current_xval, "reference_shape")),
        f("XV11", "current_xval_intervals", "requires reference literal current True",
          'reference.get("current") is not True',
          'reference.get("current") != True', p(probe_current_xval, "reference_current")),
        f("XV12", "current_xval_intervals", "requires reference SHA-256 syntax",
          'not _valid_sha256(\n                            reference.get("after_sha256"))',
          "False", p(probe_current_xval, "reference_sha")),
        f("XV13", "current_xval_intervals", "canonicalizes the reference interval",
          "_ticker, reference_interval = _canon_selection(\n                    ticker, entry.get(\"reference_interval\"))",
          "_ticker, reference_interval = (\n                    ticker, str(entry.get(\"reference_interval\")))",
          p(probe_current_xval, "reference_interval")),
        f("XV14", "current_xval_intervals", "uses injected optional fingerprint",
          "observed_reference = optional_fingerprint_fn(\n                    root, ticker, reference_interval)",
          "observed_reference = fingerprint_fn(\n                    root, ticker, reference_interval)",
          p(probe_current_xval, "valid")),
        f("XV15", "current_xval_intervals", "requires reference observation equality",
          "if (not isinstance(observed_reference, dict)\n                        or observed_reference.get(\"sha256\")\n                        != reference.get(\"after_sha256\")):",
          "if False:", p(probe_current_xval, "reference_mismatch")),
        f("XV16", "current_xval_intervals", "all fingerprint exceptions retain debt",
          "except Exception:  # noqa: BLE001 - unreadable evidence stays debt\n            continue",
          "except ManifestError:  # noqa: BLE001 - unreadable evidence stays debt\n            continue",
          p(probe_current_xval, "selected_error")),
        f("XV17", "current_xval_intervals", "deduplicates intervals in first-seen order",
          "if interval not in seen:\n            seen.add(interval)\n            out.append(interval)",
          "if True:\n            seen.add(interval)\n            out.append(interval)",
          p(probe_current_xval, "dedupe")),

        f("DS1", "discard_active", "missing active state is a no-op",
          "if not path.exists():\n            return None", "if False:\n            return None",
          p(probe_discard, "missing")),
        f("DS2", "discard_active", "uses the contained default archive",
          "if not path.exists():\n            return None\n        archive_dir = Path(archive_dir or default_archive_dir(root))",
          "if not path.exists():\n            return None\n        archive_dir = Path(archive_dir)",
          p(probe_discard, "default")),
        f("DS3", "discard_active", "creates the discard archive directory",
          "archive_dir.mkdir(parents=True, exist_ok=True)\n        stamp = _utc_text(now).replace",
          "None\n        stamp = _utc_text(now).replace", p(probe_discard, "normal")),
        f("DS4", "discard_active", "sanitizes timestamp path characters",
          '_utc_text(now).replace(":", "").replace("+", "-")',
          "_utc_text(now)", p(probe_discard, "normal")),
        f("DS5", "discard_active", "uses validated run identity",
          'identity = data["run_id"]', 'identity = "invalid"',
          p(probe_discard, "normal")),
        f("DS6", "discard_active", "falls back safely for an invalid manifest",
          "except ManifestError:\n            identity = \"invalid\"",
          "except ():\n            identity = \"invalid\"", p(probe_discard, "invalid")),
        f("DS7", "discard_active", "avoids archive-name collisions",
          "while target.exists():", "while False:", p(probe_discard, "collision")),
        f("DS8", "discard_active", "atomically moves active bytes into archive",
          "os.replace(path, target)", "os.replace(target, path)",
          p(probe_discard, "normal")),
        f("DS9", "discard_active", "normalizes move failures without data loss",
          'raise ManifestError(f"could not discard active manifest: {exc}") from exc',
          'raise OSError(f"could not discard active manifest: {exc}") from exc',
          p(probe_discard, "replace_error")),
        f("DS10", "discard_active", "returns a serializable archive path",
          "return str(target)\n", "return target\n", p(probe_discard, "normal")),
    ]
    return tuple(cases)


def redundancy_cases() -> tuple[Redundancy, ...]:
    return (
        Redundancy(
            "RR1",
            "create_run duplicate-selection guard is output-equivalent because "
            "the per-ticker interval map overwrites the same canonical key",
            mu(
                "        if item not in seen:\n"
                "            seen.add(item)\n"
                "            normalized.append(item)",
                "        if True:\n"
                "            seen.add(item)\n"
                "            normalized.append(item)",
            ),
            probe_create_duplicate_witness,
        ),
        Redundancy(
            "RR2",
            "_archive_complete outer mkdir is subsumed by _atomic_write's "
            "same-parent creation",
            mu(
                "archive_dir.mkdir(parents=True, exist_ok=True)\n"
                "    target = archive_dir / f\"addstock-run-{data['run_id']}.json\"",
                "None\n"
                "    target = archive_dir / f\"addstock-run-{data['run_id']}.json\"",
            ),
            p(probe_archive, "new"),
        ),
    )


EXPECTED_FENCE_COUNTS = {
    "active_path": 2,
    "default_archive_dir": 3,
    "_atomic_write": 9,
    "_write_manifest": 3,
    "load_run": 9,
    "create_run": 21,
    "_load_expected": 4,
    "_ticker_record": 3,
    "_save_mutation": 2,
    "mark_series_started": 8,
    "mark_series_pending": 5,
    "mark_series_complete": 7,
    "_archive_complete": 8,
    "archive_complete": 3,
    "_maybe_complete": 8,
    "exempt_empty_verification": 11,
    "mark_verification": 10,
    "mark_fetch_finished": 10,
    "replace_seal_pending": 9,
    "resume_run": 8,
    "current_xval_intervals": 17,
    "discard_active": 10,
}


def run() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    kit = CheckKit()
    kit.section("Row 85 S3 shipped-source custody and lifecycle inventory")
    before = source_bytes()
    before_raw_hash = digest(before)
    try:
        source = normalized_source(before)
        cases = fences()
        redundancies = redundancy_cases()
        mutations = tuple(
            (item.mutation.old, item.mutation.new)
            for item in (*cases, *redundancies)
        )
        owners = mutation_owners(source, mutations)
        segment_hashes = function_segment_sha256(source, owners)
        kit.check(
            "SOURCE-CUSTODY pinned per-fenced-function EOL-normalized "
            "addstock_run_manifest.py source segments",
            segment_hashes == EXPECTED_FUNCTION_SEGMENT_SHA256,
            repr(segment_hashes),
        )
        locality_source = source + (
            "" if source.endswith("\n") else "\n"
        ) + "\ndef _source_custody_unrelated_probe():\n    return None\n"
        kit.check(
            "SOURCE-CUSTODY unrelated function leaves fenced digests unchanged",
            function_segment_sha256(locality_source, owners) == segment_hashes,
        )
        probe_mutation = next(
            item.mutation for item in (*cases, *redundancies)
            if mutation_owner(
                source, item.mutation.old, item.mutation.new
            ) is not None
        )
        probe_owner = mutation_owner(
            source, probe_mutation.old, probe_mutation.new)
        kit.check(
            "SOURCE-CUSTODY fenced-function mutation changes its owned digest",
            function_segment_sha256(
                probe_mutation.apply(source), (probe_owner,)
            ) != {probe_owner: segment_hashes[probe_owner]},
            str(probe_owner),
        )
        baseline = load_module(source)
    except BaseException as exc:
        kit.check("HARNESS-INVENTORY source loads and cases build", False,
                  f"{type(exc).__name__}: {exc}")
        return kit.finish()

    ids = tuple(case.fence_id for case in cases)
    redundant_ids = tuple(case.clause_id for case in redundancies)
    actual_counts = Counter(case.function for case in cases)
    kit.check("HARNESS-INVENTORY final code-derived fence count is 170",
              len(cases) == 170, str(len(cases)))
    kit.check("HARNESS-INVENTORY per-function counts match derivation",
              dict(actual_counts) == EXPECTED_FENCE_COUNTS,
              repr(dict(actual_counts)))
    kit.check("HARNESS-INVENTORY all 22 lifecycle functions are covered",
              set(actual_counts) == set(EXPECTED_FENCE_COUNTS),
              repr(sorted(actual_counts)))
    kit.check("HARNESS-INVENTORY fence IDs are unique",
              len(set(ids)) == len(ids), repr(ids))
    kit.check("HARNESS-INVENTORY two redundant clauses are explicit",
              len(redundancies) == 2, str(len(redundancies)))
    kit.check("HARNESS-INVENTORY redundancy IDs are unique",
              len(set(redundant_ids)) == len(redundant_ids),
              repr(redundant_ids))
    kit.check("BOUNDARY _LOCK concurrency is explicitly integration-only",
              "_LOCK = threading.RLock()" in source
              and all("thread" not in case.description.lower() for case in cases))
    kit.check("CONTAINMENT fixture ticker vocabulary excludes protected names",
              SYNTHETIC_TICKERS.isdisjoint(FORBIDDEN_TICKERS),
              repr(sorted(SYNTHETIC_TICKERS & FORBIDDEN_TICKERS)))

    kit.section("One named baseline check per lifecycle fence")
    for case in cases:
        passed, detail = evaluate(case.probe, baseline)
        kit.check(
            f"FENCE {case.fence_id} {case.function} {case.description}",
            passed, detail,
        )

    kit.section("One compiling in-memory mutation killed per lifecycle fence")
    for case in cases:
        try:
            mutated_source = case.mutation.apply(source)
            mutant = load_module(mutated_source)
        except BaseException as exc:
            kit.check(f"MUTATION-KILLED {case.fence_id}", False,
                      f"mutant did not load: {type(exc).__name__}: {exc}")
            continue
        survived, detail = evaluate(case.probe, mutant)
        kit.check(f"MUTATION-KILLED {case.fence_id}", not survived,
                  "probe survived" if survived else detail)

    kit.section("Behavior-preserving lifecycle clause deletions")
    for redundant in redundancies:
        try:
            mutated_source = redundant.mutation.apply(source)
            mutant = load_module(mutated_source)
        except BaseException as exc:
            kit.check(f"REDUNDANT-CONFIRMED {redundant.clause_id}", False,
                      f"mutant did not load: {type(exc).__name__}: {exc}")
            continue
        baseline_witness, baseline_detail = evaluate(
            redundant.witness, baseline)
        mutant_witness, mutant_detail = evaluate(redundant.witness, mutant)
        all_survived = True
        first_failure = ""
        for case in cases:
            survived, detail = evaluate(case.probe, mutant)
            if not survived:
                all_survived = False
                first_failure = f"{case.fence_id}: {detail}"
                break
        proof = baseline_witness and mutant_witness and all_survived
        detail = first_failure or baseline_detail or mutant_detail
        kit.check(
            f"REDUNDANT-CONFIRMED {redundant.clause_id} "
            f"{redundant.description}", proof, detail)

    kit.section("Filesystem containment and production custody")
    temp_root = Path(tempfile.gettempdir()).resolve()
    kit.check("CONTAINMENT every sandbox base came from system temp",
              bool(_TEMP_BASES)
              and all(_inside(temp_root, base) for base in _TEMP_BASES),
              repr(_TEMP_BASES[:3]))
    kit.check("CONTAINMENT no observed path escaped its fresh temp base",
              not _OUTSIDE, repr(_OUTSIDE))
    kit.check("CONTAINMENT all fresh temp bases were deleted",
              not _RESIDUE and all(not base.exists() for base in _TEMP_BASES),
              repr(_RESIDUE))
    after = source_bytes()
    kit.check("SOURCE-CUSTODY production raw bytes unchanged during run",
              digest(after) == before_raw_hash, digest(after))
    after_segment_hashes = function_segment_sha256(
        normalized_source(after), owners)
    kit.check("SOURCE-CUSTODY fenced-function digests unchanged during run",
              after_segment_hashes == segment_hashes,
              repr(after_segment_hashes))
    return kit.finish()


if __name__ == "__main__":
    raise SystemExit(run())
