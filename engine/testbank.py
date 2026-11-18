"""TEST-ONLY synthetic storage banks and operation-gate isolation.

Production modules must never import this module.  ``build_bank`` writes a new,
empty fixture root through :mod:`stock_storage`'s real codecs and manifest
contract.  ``isolated_gates`` redirects process-global gate path attributes for
the full lifetime of an offline test and restores them transactionally.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import hashlib
import importlib
import os
import re
import sys
import tempfile
import threading
import types
from collections.abc import Mapping
from pathlib import Path

try:  # Package import in tools; direct import in the selftest scripts.
    from . import operation_gate
    from . import stock_storage as storage
except ImportError:  # pragma: no cover - exercised by script-style selftests
    import operation_gate
    import stock_storage as storage


TEST_ONLY = True
_MONTH_RE = re.compile(r"^([12]\d{3})-(0[1-9]|1[0-2])$")
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_ENGINE_ROOT = Path(__file__).resolve().parent
_PRODUCTION_BANK = storage.storage_root(_PROJECT_ROOT).resolve()
_COORDINATOR_KEY = (
    "_codex_testbank_gate_coordinator_"
    + hashlib.sha256(str(_ENGINE_ROOT).encode("utf-8")).hexdigest()[:16])
_coordinator_candidate = types.ModuleType(_COORDINATOR_KEY)
_coordinator_candidate.lock = threading.RLock()
_coordinator_candidate.stack = []
_GATE_COORDINATOR = sys.modules.setdefault(
    _COORDINATOR_KEY, _coordinator_candidate)
_GATE_REDIRECT_LOCK = _GATE_COORDINATOR.lock
_GATE_TARGET_STACK = _GATE_COORDINATOR.stack


class TestBankError(RuntimeError):
    """A synthetic fixture request is invalid or unsafe."""


class GateIsolationError(RuntimeError):
    """Gate attributes could not be redirected or restored safely."""


def _first_weekday(year, month):
    day = dt.date(year, month, 1)
    while day.weekday() >= 5:
        day += dt.timedelta(days=1)
    return day


def _first_weekdays(year, month, count):
    day = _first_weekday(year, month)
    out = []
    while day.month == month and len(out) < count:
        if day.weekday() < 5:
            out.append(day)
        day += dt.timedelta(days=1)
    if len(out) != count:
        raise TestBankError(
            f"{year:04d}-{month:02d} has fewer than {count} weekdays")
    return tuple(out)


def _validated_bar_count(value):
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise TestBankError("bars_per_day must be a positive integer")
    return value


def _validated_month(value):
    if not isinstance(value, str):
        raise TestBankError("month keys must be canonical YYYY-MM strings")
    match = _MONTH_RE.fullmatch(value)
    if match is None:
        raise TestBankError(f"invalid month key {value!r}")
    year, month = int(match.group(1)), int(match.group(2))
    if not 1900 <= year <= 2100:
        raise TestBankError(
            f"month year {year} is outside storage's 1900-2100 range")
    # Construction makes the calendar invariant explicit and keeps this
    # parser independent of string slicing later.
    dt.date(year, month, 1)
    return value, year, month


def _validate_capacity(interval, months, bars_per_day):
    if storage.base_interval(interval).endswith("d"):
        for _key, year, month in months:
            _first_weekdays(year, month, bars_per_day)
        return
    first, last = storage.session_window(interval)
    first_minute = first.hour * 60 + first.minute
    last_minute = last.hour * 60 + last.minute
    capacity = last_minute - first_minute + 1
    if bars_per_day > capacity:
        raise TestBankError(
            f"bars_per_day {bars_per_day} exceeds {interval} session "
            f"capacity {capacity}")


def _normalize_spec(spec, bars_per_day):
    if not isinstance(spec, Mapping) or not spec:
        raise TestBankError("spec must be a nonempty ticker mapping")
    normalized = {}
    origins = {}
    conid_owners = {}
    for raw_ticker, raw_intervals in spec.items():
        if not isinstance(raw_ticker, str) or not raw_ticker.strip():
            raise TestBankError("ticker keys must be nonempty strings")
        try:
            ticker = storage.canonical_ticker(raw_ticker)
        except storage.StorageError as exc:
            raise TestBankError(f"invalid ticker {raw_ticker!r}: {exc}") from exc
        if ticker in normalized:
            raise TestBankError(
                f"ticker keys {origins[ticker]!r} and {raw_ticker!r} "
                f"collide at {ticker!r}")
        if not isinstance(raw_intervals, Mapping) or not raw_intervals:
            raise TestBankError(
                f"{raw_ticker!r} must map to a nonempty interval mapping")
        intervals = {}
        for interval, raw_months in raw_intervals.items():
            if (not isinstance(interval, str)
                    or storage.INTERVAL_RE.fullmatch(interval) is None):
                raise TestBankError(f"invalid interval token {interval!r}")
            if (not isinstance(raw_months, (list, tuple))
                    or not raw_months):
                raise TestBankError(
                    f"{ticker} {interval} months must be a nonempty list")
            months = tuple(_validated_month(value) for value in raw_months)
            keys = [value[0] for value in months]
            if len(keys) != len(set(keys)):
                raise TestBankError(
                    f"{ticker} {interval} contains duplicate month keys")
            months = tuple(sorted(months))
            _validate_capacity(interval, months, bars_per_day)
            intervals[interval] = months
        conid = 20_000 + sum(ord(char) for char in ticker)
        if conid in conid_owners:
            raise TestBankError(
                f"ticker folders {conid_owners[conid]!r} and {ticker!r} "
                f"collide at synthetic conid {conid}")
        normalized[ticker] = {
            "symbol": raw_ticker.strip().upper(),
            "intervals": dict(sorted(intervals.items())),
            "conid": conid,
        }
        origins[ticker] = raw_ticker
        conid_owners[conid] = ticker
    return dict(sorted(normalized.items()))


def _prepare_root(root):
    try:
        resolved = Path(root).resolve()
    except (OSError, TypeError, ValueError) as exc:
        raise TestBankError(f"invalid fixture root: {exc}") from exc
    unsafe_roots = (_PROJECT_ROOT, _ENGINE_ROOT, _PRODUCTION_BANK)
    if any(resolved == unsafe or unsafe in resolved.parents
           for unsafe in unsafe_roots):
        raise TestBankError(
            f"refusing fixture root inside project or production data: "
            f"{resolved}")
    if resolved.exists():
        if not resolved.is_dir():
            raise TestBankError(f"fixture root is not a directory: {resolved}")
        try:
            nonempty = next(resolved.iterdir(), None) is not None
        except OSError as exc:
            raise TestBankError(f"cannot inspect fixture root: {exc}") from exc
        if nonempty:
            raise TestBankError(
                f"fixture root must be empty; refusing to merge into {resolved}")
    return resolved


def _timestamps(interval, year, month, count):
    if storage.base_interval(interval).endswith("d"):
        return tuple(
            dt.datetime.combine(day, dt.time())
            for day in _first_weekdays(year, month, count))
    day = _first_weekday(year, month)
    first, _last = storage.session_window(interval)
    start = dt.datetime.combine(day, first)
    return tuple(start + dt.timedelta(minutes=index) for index in range(count))


def _bars(interval, year, month, count, ticker_index, month_index):
    stamps = _timestamps(interval, year, month, count)
    kind = storage.kind_of(interval)
    if kind in storage.RATIO_KINDS:
        return [(stamp, 0.2, 0.2, 0.2, 0.2, 0) for stamp in stamps]

    base = 10.0 + ticker_index + month_index * 0.25
    sentinel_volume = kind == "bidask"
    rows = []
    for index, stamp in enumerate(stamps):
        opening = base + index * 0.5
        rows.append((
            stamp,
            opening,
            opening + 1.0,
            opening - 0.5,
            opening + 0.5,
            0 if sentinel_volume else 100 * (index + 1) + month_index,
        ))
    return rows


def _sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def build_bank(root, spec, *, bars_per_day=2, fmt="csv"):
    """Build a canonical synthetic bank in a new or empty fixture root.

    ``spec`` is ``{ticker: {interval_token: ["YYYY-MM", ...]}}``.  Inputs are
    fully validated and sorted before the first write.  Intraday rows begin at
    the selected session's first minute; daily rows use midnight on the first
    ``bars_per_day`` weekdays.  IV/HVOL rows use ratio OHLC and stored sentinel
    volume ``0``.  The return value maps canonical ticker folders to the raw
    SHA-256 of their saved ``manifest.json``.

    Existing content is never intentionally merged, overwritten, or deleted.
    Callers must exclusively own the fixture root for the duration of a build.
    For daily tokens, ``bars_per_day`` means consecutive weekday rows; for
    intraday tokens it means consecutive minute rows on the first weekday.
    """
    bars_per_day = _validated_bar_count(bars_per_day)
    if not isinstance(fmt, str) or fmt not in {"csv", "parquet"}:
        raise TestBankError("fmt must be 'csv' or 'parquet'")
    normalized = _normalize_spec(spec, bars_per_day)
    root = _prepare_root(root)
    root.mkdir(parents=True, exist_ok=True)

    fingerprints = {}
    for ticker_index, (ticker, item) in enumerate(normalized.items()):
        ticker_dir = root / ticker
        ticker_dir.mkdir(parents=True, exist_ok=False)
        manifest = storage.new_manifest(item["symbol"], ticker)
        manifest["conid"] = item["conid"]
        for interval, months in item["intervals"].items():
            manifest_months = storage.manifest_months(manifest, interval)
            for month_index, (key, year, month) in enumerate(months):
                path = storage.month_file_path(
                    root, ticker, year, month, interval, fmt=fmt)
                stats = storage.write_month_file(
                    path,
                    _bars(
                        interval, year, month, bars_per_day,
                        ticker_index, month_index),
                )
                manifest_months[key] = dict(stats, status="present")
        storage.save_manifest(ticker_dir, manifest)
        fingerprints[ticker] = _sha256(ticker_dir / storage.MANIFEST_NAME)
    return fingerprints


def _module_source(module):
    source = getattr(module, "__file__", None)
    if source is None:
        return None
    try:
        return Path(source).resolve()
    except (OSError, TypeError, ValueError):
        return None


def _operation_gate_modules():
    """Return every import identity backed by operation_gate.py.

    Test processes sometimes mix ``operation_gate`` and
    ``engine.operation_gate`` imports, which creates two module registries.
    Import both known identities when they are available, then include any
    other already-loaded identity backed by the same source file.
    """
    source = _module_source(operation_gate)
    if source is None:
        raise GateIsolationError("operation_gate has no resolvable source file")
    modules = [operation_gate]
    names = {"operation_gate", "engine.operation_gate",
             getattr(operation_gate, "__name__", "")}
    if __package__:
        names.add(f"{__package__}.operation_gate")
    for name in sorted(names - {""}):
        try:
            candidate = importlib.import_module(name)
        except ModuleNotFoundError as exc:
            if exc.name == name or name.startswith(f"{exc.name}."):
                continue
            raise GateIsolationError(
                f"could not load operation-gate identity {name!r}") from exc
        except ImportError as exc:
            raise GateIsolationError(
                f"could not load operation-gate identity {name!r}") from exc
        candidate_source = _module_source(candidate)
        if candidate_source != source:
            raise GateIsolationError(
                f"operation-gate identity {name!r} resolves to foreign "
                f"source {candidate_source!s}; expected {source!s}")
        modules.append(candidate)
    for candidate in tuple(sys.modules.values()):
        if (isinstance(candidate, types.ModuleType)
                and _module_source(candidate) == source):
            modules.append(candidate)
    unique = {}
    for module in modules:
        unique[id(module)] = module
    return list(unique.values())


def _target_label(owner, attr):
    return f"{getattr(owner, '__name__', repr(owner))}.{attr}"


def _gate_targets(extra_targets):
    raw = [(module, "LOCK_PATH") for module in _operation_gate_modules()]
    if _GATE_TARGET_STACK:
        raw.extend(_GATE_TARGET_STACK[-1])
    raw.extend(extra_targets)
    targets = []
    seen = set()
    for item in raw:
        if not isinstance(item, (tuple, list)) or len(item) != 2:
            raise GateIsolationError(
                "gate targets must be (object, attribute-name) pairs")
        owner, attr = item
        if not isinstance(owner, types.ModuleType):
            raise GateIsolationError(
                "gate target owners must be imported modules")
        if not isinstance(attr, str) or not attr:
            raise GateIsolationError("gate target attribute must be nonempty")
        key = (id(owner), attr)
        if key in seen:
            continue
        if not hasattr(owner, attr):
            raise GateIsolationError(f"gate target lacks attribute {attr!r}")
        original = getattr(owner, attr)
        try:
            os.fspath(original)
        except TypeError as exc:
            raise GateIsolationError(
                f"gate target {attr!r} is not path-like") from exc
        seen.add(key)
        targets.append((owner, attr, original))
    return targets


def _restore_targets(applied):
    errors = []
    for owner, attr, original in reversed(applied):
        try:
            setattr(owner, attr, original)
        except BaseException as exc:  # noqa: BLE001 - restore every target
            errors.append((owner, attr, exc))
    return errors


def _restore_error_detail(errors):
    return "; ".join(
        f"{_target_label(owner, attr)}: {type(exc).__name__}: {exc}"
        for owner, attr, exc in errors)


@contextlib.contextmanager
def isolated_gates(*extra_targets):
    """Redirect operation-gate path attributes to one temporary lock path.

    Extra targets are ``(module, "attribute")`` pairs.  All targets are
    validated before mutation, exact duplicate slots are deduplicated, nested
    contexts inherit the outer target union, and originals are restored in
    reverse order before the temporary directory is removed.  Every loaded
    import identity backed by this project's ``operation_gate.py`` is patched;
    callers must pass any other module alias explicitly.

    A process-global reentrant lock covers the *entire* context.  This permits
    same-thread nesting while preventing two threads from interleaving global
    path assignments and briefly exposing the production gate.  Enter before
    starting gate-using workers, release every lease and join those workers
    before exit, and only nest isolation contexts in the same thread.  Do not
    mutate ``sys.path`` or load a fresh duplicate gate module inside the body.

    Ordinary module attributes are restored exactly.  A hostile module that
    refuses restoration produces a fatal ``GateIsolationError`` with details;
    restoration is still attempted for every other target.
    """
    with _GATE_REDIRECT_LOCK:
        targets = _gate_targets(extra_targets)
        with tempfile.TemporaryDirectory(prefix="test-operation-gate-") as temp:
            lock_path = (Path(temp) / ".market_data_operation.lock").resolve()
            applied = []
            slots = None
            push_attempted = False
            stack_depth = None
            entered = False
            caught = None
            try:
                for owner, attr, original in targets:
                    applied.append((owner, attr, original))
                    setattr(owner, attr, lock_path)

                slots = [(owner, attr) for owner, attr, _original in targets]
                stack_depth = len(_GATE_TARGET_STACK)
                push_attempted = True
                _GATE_TARGET_STACK.append(slots)
                entered = True
                yield lock_path
            except BaseException as exc:  # noqa: BLE001 - restore then reraises
                caught = (exc, exc.__traceback__)
            finally:
                stack_error = None
                if push_attempted:
                    try:
                        if (len(_GATE_TARGET_STACK) > stack_depth
                                and _GATE_TARGET_STACK[-1] is slots):
                            _GATE_TARGET_STACK.pop()
                        elif (entered
                              or len(_GATE_TARGET_STACK) != stack_depth):
                            stack_error = GateIsolationError(
                                "gate isolation nesting stack is inconsistent")
                    except BaseException as exc:  # noqa: BLE001
                        stack_error = GateIsolationError(
                            "gate isolation nesting stack could not be restored: "
                            f"{type(exc).__name__}: {exc}")
                restore_errors = _restore_targets(applied)
                if stack_error is not None or restore_errors:
                    details = []
                    if stack_error is not None:
                        details.append(str(stack_error))
                    if restore_errors:
                        details.append(_restore_error_detail(restore_errors))
                    problem = GateIsolationError(
                        "operation-gate restoration failed: "
                        + "; ".join(details))
                    if caught is not None:
                        raise problem from caught[0]
                    raise problem
            if caught is not None:
                error, traceback = caught
                if not entered:
                    raise GateIsolationError(
                        "could not redirect operation gate") from error
                raise error.with_traceback(traceback)


__all__ = [
    "GateIsolationError",
    "TEST_ONLY",
    "TestBankError",
    "build_bank",
    "isolated_gates",
]
