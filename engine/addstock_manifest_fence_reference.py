"""Offline mutation reference for the Add Stocks manifest validator core.

The harness executes a newline-normalized, in-memory copy of the shipped
``addstock_run_manifest.py`` source.  It never edits or imports that module by
name, passes a storage root, opens a bank, acquires a gate, or touches a live
adapter.  Every counted fence has a named baseline probe and one compiling
source mutation that the same probe must kill.  Suspected redundant clauses
are deleted separately and must preserve the complete probe corpus.
"""

from __future__ import annotations

from collections import Counter, UserDict
import copy
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import sys
import types
from typing import Callable


ENGINE_ROOT = Path(__file__).resolve().parent
TARGET = ENGINE_ROOT / "addstock_run_manifest.py"
EXPECTED_FUNCTION_SEGMENT_SHA256 = {
    "_canon_selection": "e66f8aa216a660907d2d6e9d7b719e61f0fcda60c1a6f3cd6382ccfa203dfa1a",
    "_encode": "67361a9c183264ba747decf8afa20ca6894c64891937b72f0f76552eacf4ca6f",
    "_is_rth": "e0e7a22982a9d0ec11d8ffdcaedad9ab5cb0fbcf661bebabf6ec94cd3861eaaa",
    "_normalize_params": "1dc4fc3a3ff8343484ecf5992dd84301493be1e046c80523a9347ee29450ba01",
    "_recompute_ticker": "b044301cd71dd2bb4b27bc347706f6b2d0290233d5e79a15c0cada62f5900d7f",
    "_series_record": "a5ab3bbbab0ccc0eab0bcf2dc1c8b0e382418d97bb91255d35d44ef903bd6f15",
    "_text": "a4a229f53bea1eca9a4d828f5923b6b4ba93c5a2ea2544957f2baa01e0a1aaed",
    "_timestamp": "92e10c024f5a8b43b170a5d2a4442fb40950a198293d472498cfdc0e3cb91d83",
    "_utc_text": "ffcb0443d9aa1ea83141a56408d8642ce58fd9407717ec59bbebd561e4001d82",
    "_valid_sha256": "e62c4b4711b4f17108ca5c468057880b74143cca834f624eb916075626ea98a9",
    "_validate_series_map": "37774bef011501eefb678675948a31dad4be887fffbd5c22b8caab4018168916",
    "new_run_id": "e289d78ea7d0e32ef7c1bc549da8fd041dc24de393bf5a46ebc28bd6a4ab5c72",
    "pending_selections": "7b078e83b34b060f07ca8fe0b26c0c821bbc855a7e4700b8f9332118c69efd2f",
    "pending_selections.add": "56a90c139293a5beae227b356c4ab5585deaf9d7743b601b47b018c448228785",
    "summary": "0320ae1266993bdf690a3ccaa0922b4f4466f76a7ea886efe6c245c914d8855c",
    "validate_manifest": "92fa1aa535546e437eed39d7cdb83150acd14a04f9fe2109dd91bf4a494f1045",
}
FORBIDDEN_TICKERS = frozenset({"APO", "JCI", "OKE", "TKO", "WBD", "TMUS"})
SYNTHETIC_TICKERS = frozenset({"AAA", "BBB", "CCC", "VALID"})
BULK_SYNTHETIC_TICKERS = tuple(f"S{index:06d}" for index in range(10_000))
ALL_SYNTHETIC_TICKERS = SYNTHETIC_TICKERS | frozenset(BULK_SYNTHETIC_TICKERS)

if str(ENGINE_ROOT) not in sys.path:
    sys.path.insert(0, str(ENGINE_ROOT))

from check_kit import CheckKit  # noqa: E402
from source_segment_custody import (  # noqa: E402
    function_segment_sha256,
    mutation_owner,
    mutation_owners,
    normalize_source_bytes,
)


TEST_ONLY = True


class HarnessError(RuntimeError):
    """The shipped source no longer matches a reviewed mutation anchor."""


@dataclass(frozen=True)
class Mutation:
    old: str
    new: str

    def apply(self, source: str) -> str:
        count = source.count(self.old)
        if count != 1:
            raise HarnessError(
                f"mutation anchor count is {count}, expected 1: "
                f"{self.old[:120]!r}"
            )
        return source.replace(self.old, self.new, 1)


Probe = Callable[[types.ModuleType], bool]


@dataclass(frozen=True)
class Fence:
    fence_id: str
    function: str
    description: str
    mutation: Mutation
    probe: Probe


@dataclass(frozen=True)
class RedundantClause:
    clause_id: str
    description: str
    mutation: Mutation
    witness: Probe


def mu(old: str, new: str) -> Mutation:
    return Mutation(old, new)


def source_bytes() -> bytes:
    return TARGET.read_bytes()


def digest(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def normalized_source(payload: bytes) -> str:
    return normalize_source_bytes(payload)


def load_module(source: str) -> types.ModuleType:
    module = types.ModuleType("_addstock_manifest_fence_subject")
    module.__file__ = str(TARGET)
    module.__package__ = ""
    code = compile(source, str(TARGET), "exec")
    exec(code, module.__dict__)
    return module


def evaluate(probe: Probe, module: types.ModuleType) -> tuple[bool, str]:
    try:
        result = probe(module)
    except BaseException as exc:  # exact exception behavior is the subject
        return False, f"{type(exc).__name__}: {exc}"
    if result is True:
        return True, ""
    return False, f"probe returned {result!r}"


def raises_exact(call: Callable[[], object], error_type: type[BaseException],
                 message: str | None = None) -> bool:
    try:
        call()
    except BaseException as exc:
        return type(exc) is error_type and (
            message is None or str(exc) == message
        )
    return False


def accepts(call: Callable[[], object]) -> bool:
    try:
        call()
    except BaseException:
        return False
    return True


def with_attr(module: types.ModuleType, name: str, value: object,
              call: Callable[[], bool]) -> bool:
    previous = getattr(module, name)
    setattr(module, name, value)
    try:
        return call()
    finally:
        setattr(module, name, previous)


class TextLike:
    def __init__(self, value: str):
        self.value = value

    def __str__(self) -> str:
        return self.value

    def __len__(self) -> int:
        return len(self.value)

    def __iter__(self):
        return iter(self.value)

    def strip(self) -> str:
        return self.value.strip()


class HostileParams(dict):
    """Inherited values are valid while get() lies until dict-detached."""

    def get(self, key, default=None):
        if key == "mode":
            return "hostile"
        return super().get(key, default)


class HostileRecord(dict):
    """Records writes made through an undetached ticker-record subclass."""

    def __init__(self, value: dict[str, object]):
        super().__init__(value)
        self.writes: list[str] = []

    def __setitem__(self, key, value):
        self.writes.append(str(key))
        return super().__setitem__(key, value)


class HostileNaiveDateTime(datetime):
    """Makes the naive-UTC attachment fence independent of host timezone."""

    def astimezone(self, tz=None):
        if self.tzinfo is None:
            return datetime(1999, 1, 1, tzinfo=timezone.utc)
        return super().astimezone(tz)


STAMP = "2026-08-04T12:00:00+00:00"


def series(*, state: str = "pending", xval: bool = False,
           gaps: bool = False) -> dict[str, object]:
    return {"state": state, "xval": xval, "gaps": gaps}


def record(*, series_map: dict[str, dict[str, object]] | None = None,
           state: str = "pending", missing: list[str] | None = None,
           earliest: bool = False, note: object = "",
           seal_pending: object = False,
           updated_at: object = STAMP) -> dict[str, object]:
    return {
        "state": state,
        "updated_at": updated_at,
        "missing": [] if missing is None else list(missing),
        "seal_pending": seal_pending,
        "note": note,
        "earliest": earliest,
        "series": {"1m": series()} if series_map is None else series_map,
    }


def params_for(tickers: dict[str, dict[str, object]], **updates: object) \
        -> dict[str, object]:
    intervals = sorted({
        interval
        for item in tickers.values()
        for interval in item["series"]
    })
    kinds = sorted({
        "iv" if "-iv" in interval else
        "hvol" if "-hvol" in interval else
        "bidask" if "-bidask" in interval else "trades"
        for interval in intervals
    })
    value: dict[str, object] = {
        "mode": "add",
        "since": None,
        "intervals": intervals,
        "kinds": kinds,
        "extended": False,
        "store_daily": False,
        "ports_at_start": [],
    }
    value.update(updates)
    return value


def manifest(*, tickers: dict[str, dict[str, object]] | None = None,
             state: str = "active", fetch_finished: bool = False,
             params: dict[str, object] | None = None,
             finalize: dict[str, object] | None = None) -> dict[str, object]:
    selected = {"AAA": record()} if tickers is None else tickers
    return {
        "schema": 1,
        "run_id": "addstock-synthetic",
        "created_at": STAMP,
        "updated_at": STAMP,
        "state": state,
        "fetch_finished": fetch_finished,
        "params": params_for(selected) if params is None else params,
        "tickers": selected,
        "finalize": {
            "at": None, "reason": None, "seal_pending": []
        } if finalize is None else finalize,
    }


def verified_record(*, interval: str = "1m") -> dict[str, object]:
    return record(
        series_map={interval: series(state="complete", xval=True, gaps=True)},
        state="verified", missing=[], earliest=True,
    )


def built_record(*, interval_map: dict[str, dict[str, object]] | None = None,
                 missing: list[str] | None = None,
                 earliest: bool = False) -> dict[str, object]:
    selected = interval_map or {
        "1m": series(state="complete", xval=True, gaps=True)
    }
    return record(
        series_map=selected,
        state="built",
        missing=["earliest"] if missing is None else missing,
        earliest=earliest,
    )


def complete_manifest() -> dict[str, object]:
    return manifest(
        tickers={"AAA": verified_record()},
        state="complete", fetch_finished=True,
        finalize={"at": STAMP, "reason": "complete", "seal_pending": []},
    )


def reject_manifest(mutator: Callable[[dict[str, object]], None],
                    message: str | None = None) -> Probe:
    def probe(module: types.ModuleType) -> bool:
        value = manifest()
        mutator(value)
        return raises_exact(
            lambda: module.validate_manifest(value),
            module.ManifestError, message,
        )
    return probe


def oversized_manifest() -> dict[str, object]:
    tickers: dict[str, dict[str, object]] = {}
    for ticker in BULK_SYNTHETIC_TICKERS:
        tickers[ticker] = record(note="n" * 400)
    return manifest(tickers=tickers)


def f(fence_id: str, function: str, description: str,
      old: str, new: str, probe: Probe) -> Fence:
    return Fence(fence_id, function, description, mu(old, new), probe)


def p_utc_naive(module: types.ModuleType) -> bool:
    value = HostileNaiveDateTime(2026, 8, 4, 12, 0)
    return module._utc_text(value) == STAMP


def p_text_length(module: types.ModuleType) -> bool:
    return (
        module._text("abcd", "field", 4) == "abcd"
        and raises_exact(
            lambda: module._text("abcde", "field", 4),
            module.ManifestError, "field has invalid length",
        )
    )


def p_np_bounds(module: types.ModuleType) -> bool:
    base = [("AAA", "1m")]
    return (
        module._normalize_params({"ports_at_start": [1, 65535]}, base)[
            "ports_at_start"] == [1, 65535]
        and all(raises_exact(
            lambda value=value: module._normalize_params(
                {"ports_at_start": [value]}, base),
            module.ManifestError,
            "params.ports_at_start contains an invalid port",
        ) for value in (0, 65536))
    )


def p_np_cap(module: types.ModuleType) -> bool:
    return with_attr(
        module, "MAX_PORTS", 1,
        lambda: raises_exact(
            lambda: module._normalize_params(
                {"ports_at_start": [2001, 2002]}, [("AAA", "1m")]),
            module.ManifestError, "too many starting ports",
        ),
    )


def p_sm_cap(module: types.ModuleType) -> bool:
    return with_attr(
        module, "MAX_SERIES_PER_TICKER", 1,
        lambda: raises_exact(
            lambda: module._validate_series_map({
                "1m": series(), "1d": series()
            }, "AAA.series"),
            module.ManifestError, "AAA.series must be a bounded object",
        ),
    )


def p_sm_detach(module: types.ModuleType) -> bool:
    original = series()
    result = module._validate_series_map({"1m": original}, "AAA.series")
    result["1m"]["state"] = "complete"
    return result["1m"] is not original and original["state"] == "pending"


def p_vm_ticker_cap(module: types.ModuleType) -> bool:
    value = manifest(tickers={"AAA": record(), "BBB": record()})
    return with_attr(
        module, "MAX_TICKERS", 1,
        lambda: raises_exact(
            lambda: module.validate_manifest(value),
            module.ManifestError, "too many tickers",
        ),
    )


def p_vm_record_detach(module: types.ModuleType) -> bool:
    hostile = HostileRecord(record())
    value = manifest(tickers={"AAA": hostile})
    return module.validate_manifest(value) is value and not hostile.writes


def p_vm_seal_cap(module: types.ModuleType) -> bool:
    value = manifest(finalize={
        "at": None, "reason": None, "seal_pending": ["AAA 1m", "BBB 1m"]
    })
    return with_attr(
        module, "MAX_SEAL_PENDING", 1,
        lambda: raises_exact(
            lambda: module.validate_manifest(value),
            module.ManifestError,
            "finalize.seal_pending must be a bounded list",
        ),
    )


def p_encode_canonical(module: types.ModuleType) -> bool:
    value = manifest()
    raw = module._encode(value)
    expected = json.dumps(
        value, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return raw == expected and b": " not in raw and raw.startswith(b'{"created_at"')


def p_encode_cap(module: types.ModuleType) -> bool:
    value = oversized_manifest()
    return raises_exact(
        lambda: module._encode(value),
        module.ManifestError, "manifest exceeds size limit",
    )


def p_pending_preference(module: types.ModuleType) -> bool:
    preferred = built_record(
        interval_map={
            "1m-pre": series(state="complete", xval=True, gaps=True),
            "1m": series(state="complete", xval=True, gaps=True),
        },
        missing=["earliest"], earliest=False,
    )
    fallback = built_record(
        interval_map={
            "1m-pre": series(state="complete", xval=True, gaps=True),
        },
        missing=["earliest"], earliest=False,
    )
    first = module.pending_selections(manifest(tickers={"AAA": preferred}))
    second = module.pending_selections(manifest(tickers={"BBB": fallback}))
    return first == [("AAA", "1m")] and second == [("BBB", "1m-pre")]


def fences() -> tuple[Fence, ...]:
    entries: list[Fence] = []
    entries.extend((
        f("H1", "_valid_sha256", "rejects non-str string-like values",
          "return (isinstance(value, str) and len(value) == 64\n"
          "            and all(char in \"0123456789abcdef\" for char in value))",
          "return (True and len(value) == 64\n"
          "            and all(char in \"0123456789abcdef\" for char in value))",
          lambda m: not m._valid_sha256(TextLike("a" * 64))),
        f("H2", "_valid_sha256", "requires exactly 64 characters",
          "len(value) == 64", "len(value) == 63",
          lambda m: m._valid_sha256("a" * 64)
          and not m._valid_sha256("a" * 63)),
        f("H3", "_valid_sha256", "requires lowercase hexadecimal characters",
          '"0123456789abcdef"', '"0123456789abcdefg"',
          lambda m: not m._valid_sha256("g" * 64)),

        f("U1", "_utc_text", "calls an injected clock callable",
          "    value = now() if callable(now) else now\n",
          "    value = now\n",
          lambda m: m._utc_text(
              lambda: datetime(2026, 8, 4, 12, tzinfo=timezone.utc)) == STAMP),
        f("U2", "_utc_text", "uses the real UTC clock when None is supplied",
          "    if value is None:\n"
          "        value = datetime.now(timezone.utc)\n",
          "    if False:\n"
          "        value = datetime.now(timezone.utc)\n",
          lambda m: m._utc_text(None).endswith("+00:00")),
        f("U3", "_utc_text", "converts date values to UTC midnight",
          "    if isinstance(value, date) and not isinstance(value, datetime):\n",
          "    if False:\n",
          lambda m: m._utc_text(date(2026, 8, 4))
          == "2026-08-04T00:00:00+00:00"),
        f("U4", "_utc_text", "rejects non-datetime clocks as ManifestError",
          "    if not isinstance(value, datetime):\n"
          "        raise ManifestError(\"clock must return a datetime\")\n",
          "    if False:\n"
          "        raise ManifestError(\"clock must return a datetime\")\n",
          lambda m: raises_exact(
              lambda: m._utc_text("bad"), m.ManifestError,
              "clock must return a datetime")),
        f("U5", "_utc_text", "attaches UTC to naive datetimes",
          "    if value.tzinfo is None:\n"
          "        value = value.replace(tzinfo=timezone.utc)\n",
          "    if False:\n"
          "        value = value.replace(tzinfo=timezone.utc)\n",
          p_utc_naive),
        f("U6", "_utc_text", "converts aware datetimes to UTC",
          "    return value.astimezone(timezone.utc).isoformat(timespec=\"seconds\")\n",
          "    return value.isoformat(timespec=\"seconds\")\n",
          lambda m: m._utc_text(datetime(
              2026, 8, 4, 8, tzinfo=timezone(timedelta(hours=-4)))) == STAMP),
        f("U7", "_utc_text", "drops microseconds with seconds precision",
          'isoformat(timespec="seconds")', 'isoformat(timespec="microseconds")',
          lambda m: m._utc_text(datetime(
              2026, 8, 4, 12, 0, 0, 123456, tzinfo=timezone.utc)) == STAMP),

        f("I1", "new_run_id", "uses time_ns only when no value is injected",
          "    stamp = int(time.time_ns() if now_ns is None else now_ns)\n",
          "    stamp = int(now_ns if now_ns is None else time.time_ns())\n",
          lambda m: with_attr(
              m.time, "time_ns", lambda: 123,
              lambda: m.new_run_id() == "addstock-123")),
        f("I2", "new_run_id", "coerces injected stamps through int",
          "    stamp = int(time.time_ns() if now_ns is None else now_ns)\n",
          "    stamp = time.time_ns() if now_ns is None else now_ns\n",
          lambda m: m.new_run_id(1.9) == "addstock-1"),
        f("I3", "new_run_id", "emits the addstock prefix and digits",
          '    return f"addstock-{stamp}"\n', '    return f"run-{stamp}"\n',
          lambda m: m.new_run_id(7) == "addstock-7"),

        f("T1", "_text", "rejects non-str values as ManifestError",
          "    if not isinstance(value, str):\n"
          "        raise ManifestError(f\"{field} must be a string\")\n",
          "    if False:\n"
          "        raise ManifestError(f\"{field} must be a string\")\n",
          lambda m: raises_exact(
              lambda: m._text(7, "field", 8), m.ManifestError,
              "field must be a string")),
        f("T2", "_text", "strips surrounding whitespace",
          "    value = value.strip()\n", "    value = value\n",
          lambda m: m._text(" value ", "field", 8) == "value"),
        f("T3", "_text", "rejects empty text by default",
          "    if (not value and not allow_empty) or len(value) > limit:\n",
          "    if (False and not allow_empty) or len(value) > limit:\n",
          lambda m: raises_exact(
              lambda: m._text("", "field", 8), m.ManifestError,
              "field has invalid length")),
        f("T4", "_text", "allows empty text only when requested",
          "    if (not value and not allow_empty) or len(value) > limit:\n",
          "    if (not value and True) or len(value) > limit:\n",
          lambda m: m._text("", "field", 8, allow_empty=True) == ""),
        f("T5", "_text", "enforces an inclusive length limit",
          "len(value) > limit", "len(value) >= limit", p_text_length),

        f("TS1", "_timestamp", "permits None only at nullable call sites",
          "    if value is None and allow_none:\n"
          "        return None\n",
          "    if False:\n"
          "        return None\n",
          lambda m: m._timestamp(None, "at", allow_none=True) is None),
        f("TS2", "_timestamp", "delegates text normalization and bounds",
          "    value = _text(value, field, 64)\n",
          "    value = value\n",
          lambda m: m._timestamp(f" {STAMP} ", "at") == STAMP),
        f("TS3", "_timestamp", "rejects strings that are not ISO timestamps",
          "        datetime.fromisoformat(value.replace(\"Z\", \"+00:00\"))\n",
          "        pass\n",
          lambda m: raises_exact(
              lambda: m._timestamp("not-a-time", "at"), m.ManifestError,
              "at is not an ISO timestamp")),
        f("TS4", "_timestamp", "wraps ValueError as exact ManifestError",
          "        raise ManifestError(f\"{field} is not an ISO timestamp\") from exc\n",
          "        raise\n",
          lambda m: raises_exact(
              lambda: m._timestamp("not-a-time", "at"), m.ManifestError,
              "at is not an ISO timestamp")),
        f("TS5", "_timestamp", "returns the normalized original timestamp text",
          "    return value\n\n\ndef _canon_selection",
          "    return value + \"X\"\n\n\ndef _canon_selection",
          lambda m: m._timestamp(STAMP, "at") == STAMP),

        f("C1", "_canon_selection", "delegates ticker canonicalization",
          "def _canon_selection(ticker, interval):\n"
          "    try:\n"
          "        ticker = ss.canonical_ticker(str(ticker))\n",
          "def _canon_selection(ticker, interval):\n"
          "    try:\n"
          "        ticker = str(ticker)\n",
          lambda m: m._canon_selection("aaa", "1m") == ("AAA", "1m")),
        f("C2", "_canon_selection", "wraps ticker failures as ManifestError",
          "    try:\n"
          "        ticker = ss.canonical_ticker(str(ticker))\n"
          "    except Exception as exc:  # noqa: BLE001 - normalized as manifest error\n"
          "        raise ManifestError(f\"invalid ticker: {ticker!r}\") from exc\n",
          "    ticker = ss.canonical_ticker(str(ticker))\n",
          lambda m: raises_exact(
              lambda: m._canon_selection("", "1m"), m.ManifestError,
              "invalid ticker: ''")),
        f("C3", "_canon_selection", "coerces interval values to text",
          "    interval = str(interval or \"\").strip()\n",
          "    interval = interval or \"\"\n",
          lambda m: m._canon_selection("AAA", TextLike("1m"))
          == ("AAA", "1m")),
        f("C4", "_canon_selection", "normalizes None to the empty interval",
          "    interval = str(interval or \"\").strip()\n",
          "    interval = str(interval).strip()\n",
          lambda m: raises_exact(
              lambda: m._canon_selection("AAA", None), m.ManifestError,
              "invalid interval: ''")),
        f("C5", "_canon_selection", "strips interval whitespace",
          "    interval = str(interval or \"\").strip()\n",
          "    interval = str(interval or \"\")\n",
          lambda m: m._canon_selection("AAA", " 1m ") == ("AAA", "1m")),
        f("C6", "_canon_selection", "returns canonical ticker then interval",
          "    return ticker, interval\n\n\ndef _is_rth",
          "    return interval, ticker\n\n\ndef _is_rth",
          lambda m: m._canon_selection("aaa", "1m") == ("AAA", "1m")),

        f("R1", "_is_rth", "recognizes only the rth session",
          '        return ss.session_of(interval) == "rth"\n',
          '        return ss.session_of(interval) == "pre"\n',
          lambda m: m._is_rth("1m") and not m._is_rth("1m-pre")),
        f("R2", "_is_rth", "maps malformed interval exceptions to False",
          "    except Exception:  # noqa: BLE001 - validation catches malformed intervals\n",
          "    except KeyError:\n",
          lambda m: m._is_rth(None) is False),

        f("SR1", "_series_record", "starts each series in pending state",
          "def _series_record(interval):\n"
          "    needs_checks = _is_rth(interval)\n"
          "    return {\n"
          '        "state": "pending",\n',
          "def _series_record(interval):\n"
          "    needs_checks = _is_rth(interval)\n"
          "    return {\n"
          '        "state": "building",\n',
          lambda m: m._series_record("1m")["state"] == "pending"),
        f("SR2", "_series_record", "RTH series starts owing xval",
          '        "xval": not needs_checks,\n', '        "xval": True,\n',
          lambda m: not m._series_record("1m")["xval"]
          and m._series_record("1m-pre")["xval"]),
        f("SR3", "_series_record", "RTH series starts owing gaps",
          '        "gaps": not needs_checks,\n', '        "gaps": True,\n',
          lambda m: not m._series_record("1m")["gaps"]
          and m._series_record("1m-pre")["gaps"]),
    ))

    entries.extend(normalize_param_fences())
    entries.extend(series_map_fences())
    entries.extend(recompute_fences())
    entries.extend(validate_manifest_fences())
    entries.extend(encode_fences())
    entries.extend(pending_fences())
    entries.extend(summary_fences())
    return tuple(entries)


def normalize_param_fences() -> tuple[Fence, ...]:
    selections = [("AAA", "1m")]
    return (
        f("NP1", "_normalize_params", "detaches mapping subclasses before reads",
          "    params = dict(params or {})\n", "    params = params or {}\n",
          lambda m: m._normalize_params(
              HostileParams({"mode": "add"}), selections)["mode"] == "add"),
        f("NP2", "_normalize_params", "rejects unknown fields with sorted detail",
          "    if unknown:\n"
          "        raise ManifestError(f\"unsupported params: {', '.join(unknown)}\")\n",
          "    if False:\n"
          "        raise ManifestError(f\"unsupported params: {', '.join(unknown)}\")\n",
          lambda m: raises_exact(
              lambda: m._normalize_params({"z": 1, "a": 2}, selections),
              m.ManifestError, "unsupported params: a, z")),
        f("NP3", "_normalize_params", "defaults mode to add",
          '    mode = _text(str(params.get("mode") or "add"), "params.mode", 24)\n',
          '    mode = _text(str(params.get("mode") or "other"), "params.mode", 24)\n',
          lambda m: m._normalize_params({}, selections)["mode"] == "add"),
        f("NP4", "_normalize_params", "normalizes mode through _text",
          '    mode = _text(str(params.get("mode") or "add"), "params.mode", 24)\n',
          '    mode = str(params.get("mode") or "add")\n',
          lambda m: m._normalize_params(
              {"mode": " add "}, selections)["mode"] == "add"),
        f("NP5", "_normalize_params", "datetime since values become dates",
          "        since = since.date().isoformat() if isinstance(since, datetime) \\\n"
          "            else since.isoformat()\n",
          "        since = since.isoformat()\n",
          lambda m: m._normalize_params({
              "since": datetime(2026, 8, 4, 12, tzinfo=timezone.utc)
          }, selections)["since"] == "2026-08-04"),
        f("NP6", "_normalize_params", "preserves an absent since value",
          "    if since is not None:\n"
          "        since = _text(str(since), \"params.since\", 32)\n",
          "    if True:\n"
          "        since = _text(str(since), \"params.since\", 32)\n",
          lambda m: m._normalize_params({}, selections)["since"] is None),
        f("NP7", "_normalize_params", "strips textual since values",
          '        since = _text(str(since), "params.since", 32)\n',
          '        since = str(since)\n',
          lambda m: m._normalize_params({
              "since": " 2026-08-04 "
          }, selections)["since"] == "2026-08-04"),
        f("NP8", "_normalize_params", "rejects invalid ISO dates as ManifestError",
          "            date.fromisoformat(since)\n", "            pass\n",
          lambda m: raises_exact(
              lambda: m._normalize_params({"since": "not-a-date"}, selections),
              m.ManifestError, "params.since must be an ISO date")),
        f("NP9", "_normalize_params", "derives sorted unique intervals",
          "    intervals = sorted({iv for _ticker, iv in selections})\n",
          "    intervals = [iv for _ticker, iv in selections]\n",
          lambda m: m._normalize_params({}, [
              ("AAA", "1m"), ("AAA", "1d"), ("AAA", "1m")
          ])["intervals"] == ["1d", "1m"]),
        f("NP10", "_normalize_params", "strips supplied interval tokens",
          "        supplied_intervals = [str(v).strip() for v in supplied_intervals]\n",
          "        supplied_intervals = [str(v) for v in supplied_intervals]\n",
          lambda m: m._normalize_params({
              "intervals": [" 1m "]
          }, selections)["intervals"] == ["1m"]),
        f("NP11", "_normalize_params", "requires supplied intervals to match exactly",
          "        if supplied_intervals != intervals:\n"
          "            raise ManifestError(\"params.intervals does not match selections\")\n",
          "        if False:\n"
          "            raise ManifestError(\"params.intervals does not match selections\")\n",
          lambda m: raises_exact(
              lambda: m._normalize_params({"intervals": ["1d"]}, selections),
              m.ManifestError, "params.intervals does not match selections")),
        f("NP12", "_normalize_params", "derives sorted unique kinds with trades fallback",
          '    kinds = sorted({ss.kind_of(iv) or "trades" for iv in intervals})\n',
          '    kinds = [ss.kind_of(iv) or "trades" for iv in intervals]\n',
          lambda m: m._normalize_params({}, [
              ("AAA", "1m"), ("AAA", "1d"), ("AAA", "1d-iv")
          ])["kinds"] == ["iv", "trades"]),
        f("NP13", "_normalize_params", "pins supplied-kinds no-strip behavior",
          "    if supplied_kinds is not None and [str(v) for v in supplied_kinds] != kinds:\n",
          "    if supplied_kinds is not None and [str(v).strip() for v in supplied_kinds] != kinds:\n",
          lambda m: raises_exact(
              lambda: m._normalize_params({"kinds": [" trades"]}, selections),
              m.ManifestError, "params.kinds does not match selections")),
        f("NP14", "_normalize_params", "requires supplied kinds to match exactly",
          "    if supplied_kinds is not None and [str(v) for v in supplied_kinds] != kinds:\n"
          "        raise ManifestError(\"params.kinds does not match selections\")\n",
          "    if False:\n"
          "        raise ManifestError(\"params.kinds does not match selections\")\n",
          lambda m: raises_exact(
              lambda: m._normalize_params({"kinds": ["iv"]}, selections),
              m.ManifestError, "params.kinds does not match selections")),
        f("NP15", "_normalize_params", "coerces starting ports through int",
          "            port = int(value)\n", "            port = value\n",
          lambda m: m._normalize_params({
              "ports_at_start": ["2001"]
          }, selections)["ports_at_start"] == [2001]),
        f("NP16", "_normalize_params", "wraps non-port conversion failures",
          "            raise ManifestError(\"params.ports_at_start contains a non-port\") from exc\n",
          "            raise\n",
          lambda m: raises_exact(
              lambda: m._normalize_params(
                  {"ports_at_start": ["bad"]}, selections),
              m.ManifestError, "params.ports_at_start contains a non-port")),
        f("NP17", "_normalize_params", "enforces inclusive TCP port bounds",
          "        if not 1 <= port <= 65535:\n"
          "            raise ManifestError(\"params.ports_at_start contains an invalid port\")\n",
          "        if False:\n"
          "            raise ManifestError(\"params.ports_at_start contains an invalid port\")\n",
          p_np_bounds),
        f("NP18", "_normalize_params", "deduplicates ports in first-seen order",
          "        if port not in ports:\n"
          "            ports.append(port)\n",
          "        if True:\n"
          "            ports.append(port)\n",
          lambda m: m._normalize_params({
              "ports_at_start": [2002, 2001, 2002]
          }, selections)["ports_at_start"] == [2002, 2001]),
        f("NP19", "_normalize_params", "caps the number of starting ports",
          "    if len(ports) > MAX_PORTS:\n"
          "        raise ManifestError(\"too many starting ports\")\n",
          "    if False:\n"
          "        raise ManifestError(\"too many starting ports\")\n",
          p_np_cap),
        f("NP20", "_normalize_params", "coerces both boolean policy flags",
          '        "extended": bool(params.get("extended", False)),\n'
          '        "store_daily": bool(params.get("store_daily", False)),\n',
          '        "extended": bool(params.get("store_daily", False)),\n'
          '        "store_daily": bool(params.get("extended", False)),\n',
          lambda m: (
              lambda value: value["extended"] is True
              and value["store_daily"] is False
          )(m._normalize_params({
              "extended": 1, "store_daily": 0
          }, selections))),
    )


def series_map_fences() -> tuple[Fence, ...]:
    bounded_guard = (
        "    if not isinstance(value, dict) or len(value) > MAX_SERIES_PER_TICKER:\n"
        "        raise ManifestError(f\"{field} must be a bounded object\")\n"
    )
    record_guard = (
        "        if not isinstance(raw, dict) or set(raw) != {\"state\", \"xval\", \"gaps\"}:\n"
        "            raise ManifestError(f\"{field}.{interval} has invalid fields\")\n"
    )
    flags_guard = (
        "        if not isinstance(raw.get(\"xval\"), bool) or not isinstance(\n"
        "                raw.get(\"gaps\"), bool):\n"
        "            raise ManifestError(f\"{field}.{interval} stage flags must be booleans\")\n"
    )
    return (
        f("SM1", "_validate_series_map", "requires a dict series map",
          bounded_guard,
          bounded_guard.replace("if not isinstance(value, dict) or", "if False or"),
          lambda m: raises_exact(
              lambda: m._validate_series_map(["bad"], "AAA.series"),
              m.ManifestError, "AAA.series must be a bounded object")),
        f("SM2", "_validate_series_map", "caps series per ticker",
          bounded_guard,
          bounded_guard.replace("len(value) > MAX_SERIES_PER_TICKER", "False"),
          p_sm_cap),
        f("SM3", "_validate_series_map", "canonicalizes and validates interval keys",
          '        _ticker, interval = _canon_selection("VALID", interval)\n',
          '        _ticker, interval = "VALID", str(interval)\n',
          lambda m: raises_exact(
              lambda: m._validate_series_map({"bad": series()}, "AAA.series"),
              m.ManifestError, "invalid interval: 'bad'")),
        f("SM4", "_validate_series_map", "requires each series record to be a dict",
          record_guard,
          record_guard.replace("if not isinstance(raw, dict) or", "if False or"),
          lambda m: raises_exact(
              lambda: m._validate_series_map({
                  "1m": ["state", "xval", "gaps"]
              }, "AAA.series"),
              m.ManifestError, "AAA.series.1m has invalid fields")),
        f("SM5", "_validate_series_map", "requires exact series record fields",
          record_guard,
          record_guard.replace(
              'set(raw) != {"state", "xval", "gaps"}', "False"),
          lambda m: raises_exact(
              lambda: m._validate_series_map({
                  "1m": {**series(), "extra": 1}
              }, "AAA.series"),
              m.ManifestError, "AAA.series.1m has invalid fields")),
        f("SM6", "_validate_series_map", "enforces the series-state vocabulary",
          "        if state not in SERIES_STATES:\n"
          "            raise ManifestError(f\"{field}.{interval}.state is unsupported\")\n",
          "        if False:\n"
          "            raise ManifestError(f\"{field}.{interval}.state is unsupported\")\n",
          lambda m: raises_exact(
              lambda: m._validate_series_map({
                  "1m": series(state="bad")
              }, "AAA.series"),
              m.ManifestError, "AAA.series.1m.state is unsupported")),
        f("SM7", "_validate_series_map", "requires xval to be a real bool",
          flags_guard,
          flags_guard.replace(
              'not isinstance(raw.get("xval"), bool)', "False"),
          lambda m: raises_exact(
              lambda: m._validate_series_map({
                  "1m": series(xval=1)
              }, "AAA.series"),
              m.ManifestError, "AAA.series.1m stage flags must be booleans")),
        f("SM8", "_validate_series_map", "requires gaps to be a real bool",
          flags_guard,
          flags_guard.replace(
              'not isinstance(\n                raw.get("gaps"), bool)', "False"),
          lambda m: raises_exact(
              lambda: m._validate_series_map({
                  "1m": series(gaps=1)
              }, "AAA.series"),
              m.ManifestError, "AAA.series.1m stage flags must be booleans")),
        f("SM9", "_validate_series_map", "forbids debt on non-RTH sessions",
          "        if not _is_rth(interval) and not (raw[\"xval\"] and raw[\"gaps\"]):\n"
          "            raise ManifestError(f\"{field}.{interval} cannot owe RTH checks\")\n",
          "        if False:\n"
          "            raise ManifestError(f\"{field}.{interval} cannot owe RTH checks\")\n",
          lambda m: raises_exact(
              lambda: m._validate_series_map({
                  "1m-pre": series(xval=False, gaps=False)
              }, "AAA.series"),
              m.ManifestError, "AAA.series.1m-pre cannot owe RTH checks")),
        f("SM10", "_validate_series_map", "detaches accepted series records",
          "        out[interval] = dict(raw)\n",
          "        out[interval] = raw\n", p_sm_detach),
        f("SM11", "_validate_series_map", "rejects an empty series map",
          "    if not out:\n"
          "        raise ManifestError(f\"{field} cannot be empty\")\n",
          "    if False:\n"
          "        raise ManifestError(f\"{field} cannot be empty\")\n",
          lambda m: raises_exact(
              lambda: m._validate_series_map({}, "AAA.series"),
              m.ManifestError, "AAA.series cannot be empty")),
    )


def recompute_fences() -> tuple[Fence, ...]:
    return (
        f("RC1", "_recompute_ticker", "distinguishes incomplete from complete series",
          '    complete = all(v["state"] == "complete" for v in series.values())\n',
          "    complete = True\n",
          lambda m: (
              lambda value: (m._recompute_ticker(value), value)[1]["state"]
          )(record()) == "pending"),
        f("RC2", "_recompute_ticker", "marks any started incomplete series building",
          '            "building" if any(v["state"] != "pending" for v in series.values())\n',
          '            "building" if False\n',
          lambda m: (
              lambda value: (m._recompute_ticker(value), value)[1]["state"]
          )(record(series_map={"1m": series(state="building")})) == "building"),
        f("RC3", "_recompute_ticker", "keeps wholly pending work pending",
          '            else "pending"\n', '            else "built"\n',
          lambda m: (
              lambda value: (m._recompute_ticker(value), value)[1]["state"]
          )(record()) == "pending"),
        f("RC4", "_recompute_ticker", "clears stale debt while work is incomplete",
          '        record["missing"] = []\n        return\n',
          '        record["missing"] = record["missing"]\n        return\n',
          lambda m: (
              lambda value: (m._recompute_ticker(value), value)[1]["missing"]
          )(record(missing=["earliest"])) == []),
        f("RC5", "_recompute_ticker", "returns before debt calculation when incomplete",
          '        record["missing"] = []\n        return\n',
          '        record["missing"] = []\n        pass\n',
          lambda m: (
              lambda value: (m._recompute_ticker(value), value)[1]["state"]
          )(record()) == "pending"),
        f("RC6", "_recompute_ticker", "computes xval and gaps debt from RTH only",
          "    rth = [v for iv, v in series.items() if _is_rth(iv)]\n",
          "    rth = [v for iv, v in series.items() if not _is_rth(iv)]\n",
          lambda m: (
              lambda value: (m._recompute_ticker(value), value)[1]
          )(record(
              series_map={
                  "1m-pre": series(state="complete", xval=False, gaps=False)
              }, earliest=True
          ))["state"] == "verified"),
        f("RC7", "_recompute_ticker", "records xval debt first",
          '    if any(not v["xval"] for v in rth):\n'
          '        missing.append("xval")\n',
          "    if False:\n"
          '        missing.append("xval")\n',
          lambda m: (
              lambda value: (m._recompute_ticker(value), value)[1]["missing"]
          )(record(
              series_map={
                  "1m": series(state="complete", xval=False, gaps=True)
              }, earliest=True
          )) == ["xval"]),
        f("RC8", "_recompute_ticker", "records gaps debt after xval",
          '    if any(not v["gaps"] for v in rth):\n'
          '        missing.append("gaps")\n',
          "    if False:\n"
          '        missing.append("gaps")\n',
          lambda m: (
              lambda value: (m._recompute_ticker(value), value)[1]["missing"]
          )(record(
              series_map={
                  "1m": series(state="complete", xval=False, gaps=False)
              }, earliest=True
          )) == ["xval", "gaps"]),
        f("RC9", "_recompute_ticker", "earliest debt controls built versus verified",
          '    if not record["earliest"]:\n'
          '        missing.append("earliest")\n',
          "    if False:\n"
          '        missing.append("earliest")\n',
          lambda m: (
              lambda value: (m._recompute_ticker(value), value)[1]
          )(record(
              series_map={
                  "1m": series(state="complete", xval=True, gaps=True)
              }, earliest=False
          ))["missing"] == ["earliest"]),
    )


def p_vm_root_fields(module: types.ModuleType) -> bool:
    missing = manifest()
    missing.pop("created_at")
    extra = manifest()
    extra["extra"] = 1
    return all(raises_exact(
        lambda value=value: module.validate_manifest(value),
        module.ManifestError, "manifest root fields do not match schema 1",
    ) for value in (missing, extra))


def p_vm_complete_state_gate(module: types.ModuleType) -> bool:
    value = manifest()
    return module.validate_manifest(value) is value


def p_vm_complete_fetch(module: types.ModuleType) -> bool:
    value = manifest(
        tickers={"AAA": verified_record()},
        state="complete", fetch_finished=False,
        finalize={"at": STAMP, "reason": "complete", "seal_pending": []},
    )
    return raises_exact(
        lambda: module.validate_manifest(value),
        module.ManifestError, "complete run has unfinished work",
    )


def p_vm_complete_verified(module: types.ModuleType) -> bool:
    value = manifest(
        tickers={"AAA": built_record()},
        state="complete", fetch_finished=True,
        finalize={"at": STAMP, "reason": "complete", "seal_pending": []},
    )
    return raises_exact(
        lambda: module.validate_manifest(value),
        module.ManifestError, "complete run has unfinished work",
    )


def validate_manifest_fences() -> tuple[Fence, ...]:
    root_guard = (
        "    if not isinstance(data, dict):\n"
        "        raise ManifestError(\"manifest root must be an object\")\n"
    )
    ticker_guard = (
        "    if not isinstance(data.get(\"tickers\"), dict) or not data[\"tickers\"]:\n"
        "        raise ManifestError(\"tickers must be a non-empty object\")\n"
    )
    record_guard = (
        "        if canon != ticker or not isinstance(record, dict):\n"
        "            raise ManifestError(f\"invalid ticker record: {ticker!r}\")\n"
    )
    missing_guard = (
        "        if len(missing) != len(set(missing)) or any(\n"
        "                value not in DEBT_STAGES for value in missing):\n"
        "            raise ManifestError(f\"{ticker}.missing is invalid\")\n"
    )
    consistency_guard = (
        "        if copy[\"state\"] != ticker_state or copy[\"missing\"] != missing:\n"
        "            raise ManifestError(f\"{ticker} derived state is inconsistent\")\n"
    )
    finalize_guard = (
        "    if not isinstance(finalize, dict) or set(finalize) != {\n"
        "            \"at\", \"reason\", \"seal_pending\"}:\n"
        "        raise ManifestError(\"finalize fields do not match schema 1\")\n"
    )
    seal_guard = (
        "    if not isinstance(seal_pending, list) or len(seal_pending) > MAX_SEAL_PENDING:\n"
        "        raise ManifestError(\"finalize.seal_pending must be a bounded list\")\n"
    )
    complete_guard = (
        "    if state == \"complete\" and (not data[\"fetch_finished\"] or any(\n"
        "            rec[\"state\"] != \"verified\" for rec in data[\"tickers\"].values())):\n"
        "        raise ManifestError(\"complete run has unfinished work\")\n"
    )
    return (
        f("VM1", "validate_manifest", "requires a dict manifest root",
          root_guard,
          root_guard.replace("if not isinstance(data, dict):", "if False:"),
          lambda m: raises_exact(
              lambda: m.validate_manifest([]), m.ManifestError,
              "manifest root must be an object")),
        f("VM2", "validate_manifest", "requires exact root fields",
          "    if set(data) != expected:\n"
          "        raise ManifestError(\"manifest root fields do not match schema 1\")\n",
          "    if False:\n"
          "        raise ManifestError(\"manifest root fields do not match schema 1\")\n",
          p_vm_root_fields),
        f("VM3", "validate_manifest", "rejects unsupported schema values",
          "    if data.get(\"schema\") != SCHEMA:\n"
          "        raise ManifestError(\"unsupported manifest schema\")\n",
          "    if False:\n"
          "        raise ManifestError(\"unsupported manifest schema\")\n",
          reject_manifest(
              lambda value: value.__setitem__("schema", 2),
              "unsupported manifest schema")),
        f("VM4", "validate_manifest", "normalizes and bounds run_id text",
          '    run_id = _text(data.get("run_id"), "run_id", 80)\n',
          '    run_id = str(data.get("run_id"))\n',
          reject_manifest(
              lambda value: value.__setitem__("run_id", 7),
              "run_id must be a string")),
        f("VM5", "validate_manifest", "requires the safe run_id regex",
          "    if not _RUN_ID_RE.fullmatch(run_id):\n"
          "        raise ManifestError(\"run_id has unsafe characters\")\n",
          "    if False:\n"
          "        raise ManifestError(\"run_id has unsafe characters\")\n",
          reject_manifest(
              lambda value: value.__setitem__("run_id", "bad/run"),
              "run_id has unsafe characters")),
        f("VM6", "validate_manifest", "validates created_at",
          '    _timestamp(data.get("created_at"), "created_at")\n',
          "    pass\n",
          reject_manifest(
              lambda value: value.__setitem__("created_at", "bad"),
              "created_at is not an ISO timestamp")),
        f("VM7", "validate_manifest", "validates updated_at",
          '    _timestamp(data.get("updated_at"), "updated_at")\n',
          "    pass\n",
          reject_manifest(
              lambda value: value.__setitem__("updated_at", "bad"),
              "updated_at is not an ISO timestamp")),
        f("VM8", "validate_manifest", "enforces the run-state vocabulary",
          "    if state not in RUN_STATES:\n"
          "        raise ManifestError(\"unsupported run state\")\n",
          "    if False:\n"
          "        raise ManifestError(\"unsupported run state\")\n",
          reject_manifest(
              lambda value: value.__setitem__("state", "bad"),
              "unsupported run state")),
        f("VM9", "validate_manifest", "requires fetch_finished to be bool",
          "    if not isinstance(data.get(\"fetch_finished\"), bool):\n"
          "        raise ManifestError(\"fetch_finished must be a boolean\")\n",
          "    if False:\n"
          "        raise ManifestError(\"fetch_finished must be a boolean\")\n",
          reject_manifest(
              lambda value: value.__setitem__("fetch_finished", 1),
              "fetch_finished must be a boolean")),
        f("VM10", "validate_manifest", "requires tickers to be a real dict",
          ticker_guard,
          ticker_guard.replace(
              'if not isinstance(data.get("tickers"), dict) or', "if False or"),
          lambda m: (
              lambda value: raises_exact(
                  lambda: m.validate_manifest(value), m.ManifestError,
                  "tickers must be a non-empty object"
              )
          )((lambda value: (
              value.__setitem__("tickers", UserDict(value["tickers"])), value
          )[1])(manifest()))),
        f("VM11", "validate_manifest", "requires at least one ticker",
          ticker_guard,
          ticker_guard.replace('or not data["tickers"]', "or False"),
          reject_manifest(
              lambda value: (
                  value.__setitem__("tickers", {}),
                  value.__setitem__("params", params_for({})),
              ),
              "tickers must be a non-empty object")),
        f("VM12", "validate_manifest", "caps ticker cardinality",
          "    if len(data[\"tickers\"]) > MAX_TICKERS:\n"
          "        raise ManifestError(\"too many tickers\")\n",
          "    if False:\n"
          "        raise ManifestError(\"too many tickers\")\n",
          p_vm_ticker_cap),
        f("VM13", "validate_manifest", "requires ticker keys to be canonical",
          record_guard,
          record_guard.replace("if canon != ticker or", "if False or"),
          lambda m: (
              lambda value: raises_exact(
                  lambda: m.validate_manifest(value), m.ManifestError,
                  "invalid ticker record: 'aaa'"
              )
          )(manifest(tickers={"aaa": record()}))),
        f("VM14", "validate_manifest", "requires ticker records to be real dicts",
          record_guard,
          record_guard.replace("or not isinstance(record, dict)", "or False"),
          lambda m: (
              lambda value: raises_exact(
                  lambda: m.validate_manifest(value), m.ManifestError,
                  "invalid ticker record: 'AAA'"
              )
          )((lambda value: (
              value["tickers"].__setitem__(
                  "AAA", UserDict(value["tickers"]["AAA"])), value
          )[1])(manifest()))),
        f("VM15", "validate_manifest", "requires exact ticker-record fields",
          "        if set(record) != fields:\n"
          "            raise ManifestError(f\"{ticker} fields do not match schema 1\")\n",
          "        if False:\n"
          "            raise ManifestError(f\"{ticker} fields do not match schema 1\")\n",
          reject_manifest(
              lambda value: value["tickers"]["AAA"].__setitem__("extra", 1),
              "AAA fields do not match schema 1")),
        f("VM16", "validate_manifest", "enforces ticker-state vocabulary",
          "        if ticker_state not in TICKER_STATES:\n"
          "            raise ManifestError(f\"{ticker}.state is unsupported\")\n",
          "        if False:\n"
          "            raise ManifestError(f\"{ticker}.state is unsupported\")\n",
          reject_manifest(
              lambda value: value["tickers"]["AAA"].__setitem__("state", "bad"),
              "AAA.state is unsupported")),
        f("VM17", "validate_manifest", "validates each ticker updated_at",
          '        _timestamp(record.get("updated_at"), f"{ticker}.updated_at")\n',
          "        pass\n",
          reject_manifest(
              lambda value: value["tickers"]["AAA"].__setitem__(
                  "updated_at", "bad"),
              "AAA.updated_at is not an ISO timestamp")),
        f("VM18", "validate_manifest", "requires missing debt to be a list",
          "        if not isinstance(record.get(\"missing\"), list):\n"
          "            raise ManifestError(f\"{ticker}.missing must be a list\")\n",
          "        if False:\n"
          "            raise ManifestError(f\"{ticker}.missing must be a list\")\n",
          reject_manifest(
              lambda value: value["tickers"]["AAA"].__setitem__("missing", ()),
              "AAA.missing must be a list")),
        f("VM19", "validate_manifest", "rejects duplicate missing stages",
          missing_guard,
          missing_guard.replace(
              "if len(missing) != len(set(missing)) or", "if False or"),
          reject_manifest(
              lambda value: value["tickers"]["AAA"].__setitem__(
                  "missing", ["earliest", "earliest"]),
              "AAA.missing is invalid")),
        f("VM20", "validate_manifest", "enforces the missing-stage vocabulary",
          missing_guard,
          missing_guard.replace(
              "any(\n                value not in DEBT_STAGES for value in missing)",
              "False"),
          reject_manifest(
              lambda value: value["tickers"]["AAA"].__setitem__(
                  "missing", ["unknown"]),
              "AAA.missing is invalid")),
        f("VM21", "validate_manifest", "requires ticker seal_pending bool",
          "        if not isinstance(record.get(\"seal_pending\"), bool):\n"
          "            raise ManifestError(f\"{ticker}.seal_pending must be a boolean\")\n",
          "        if False:\n"
          "            raise ManifestError(f\"{ticker}.seal_pending must be a boolean\")\n",
          reject_manifest(
              lambda value: value["tickers"]["AAA"].__setitem__(
                  "seal_pending", 1),
              "AAA.seal_pending must be a boolean")),
        f("VM22", "validate_manifest", "validates bounded ticker notes",
          '        _text(record.get("note"), f"{ticker}.note", MAX_NOTE, allow_empty=True)\n',
          "        pass\n",
          reject_manifest(
              lambda value: value["tickers"]["AAA"].__setitem__("note", 1),
              "AAA.note must be a string")),
        f("VM23", "validate_manifest", "requires earliest bool",
          "        if not isinstance(record.get(\"earliest\"), bool):\n"
          "            raise ManifestError(f\"{ticker}.earliest must be a boolean\")\n",
          "        if False:\n"
          "            raise ManifestError(f\"{ticker}.earliest must be a boolean\")\n",
          reject_manifest(
              lambda value: value["tickers"]["AAA"].__setitem__("earliest", 1),
              "AAA.earliest must be a boolean")),
        f("VM24", "validate_manifest", "delegates series-map validation",
          '        series = _validate_series_map(record.get("series"), f"{ticker}.series")\n',
          '        series = record.get("series")\n',
          reject_manifest(
              lambda value: value["tickers"]["AAA"]["series"]["1m"].__setitem__(
                  "state", "bad"),
              "AAA.series.1m.state is unsupported")),
        f("VM25", "validate_manifest", "detaches ticker records before recompute",
          "        copy = dict(record)\n", "        copy = record\n",
          p_vm_record_detach),
        f("VM26", "validate_manifest", "checks derived ticker state",
          consistency_guard,
          consistency_guard.replace(
              'if copy["state"] != ticker_state or', "if False or"),
          reject_manifest(
              lambda value: value["tickers"]["AAA"].__setitem__("state", "built"),
              "AAA derived state is inconsistent")),
        f("VM27", "validate_manifest", "checks derived missing debt",
          consistency_guard,
          consistency_guard.replace(
              'copy["missing"] != missing', "False"),
          reject_manifest(
              lambda value: value["tickers"]["AAA"].__setitem__(
                  "missing", ["earliest"]),
              "AAA derived state is inconsistent")),
        f("VM28", "validate_manifest", "requires params to equal normalization",
          "    if normalized_params != data[\"params\"]:\n"
          "        raise ManifestError(\"params are not normalized\")\n",
          "    if False:\n"
          "        raise ManifestError(\"params are not normalized\")\n",
          reject_manifest(
              lambda value: value["params"].__setitem__("mode", " add "),
              "params are not normalized")),
        f("VM29", "validate_manifest", "requires finalize to be a real dict",
          finalize_guard,
          finalize_guard.replace(
              "if not isinstance(finalize, dict) or", "if False or"),
          lambda m: (
              lambda value: raises_exact(
                  lambda: m.validate_manifest(value), m.ManifestError,
                  "finalize fields do not match schema 1"
              )
          )((lambda value: (
              value.__setitem__("finalize", UserDict(value["finalize"])), value
          )[1])(manifest()))),
        f("VM30", "validate_manifest", "requires exact finalize fields",
          finalize_guard,
          finalize_guard.replace(
              'set(finalize) != {\n            "at", "reason", "seal_pending"}',
              "False"),
          reject_manifest(
              lambda value: value["finalize"].__setitem__("extra", 1),
              "finalize fields do not match schema 1")),
        f("VM31", "validate_manifest", "validates nullable finalize.at",
          '    _timestamp(finalize.get("at"), "finalize.at", allow_none=True)\n',
          "    pass\n",
          reject_manifest(
              lambda value: value["finalize"].__setitem__("at", "bad"),
              "finalize.at is not an ISO timestamp")),
        f("VM32", "validate_manifest", "validates non-null finalize reason text",
          "    if reason is not None:\n"
          "        _text(reason, \"finalize.reason\", 80)\n",
          "    if False:\n"
          "        _text(reason, \"finalize.reason\", 80)\n",
          reject_manifest(
              lambda value: value["finalize"].__setitem__("reason", 1),
              "finalize.reason must be a string")),
        f("VM33", "validate_manifest", "requires seal_pending to be a list",
          seal_guard,
          seal_guard.replace(
              "if not isinstance(seal_pending, list) or", "if False or"),
          reject_manifest(
              lambda value: value["finalize"].__setitem__("seal_pending", ()),
              "finalize.seal_pending must be a bounded list")),
        f("VM34", "validate_manifest", "caps finalize seal debt",
          seal_guard,
          seal_guard.replace(
              "len(seal_pending) > MAX_SEAL_PENDING", "False"),
          p_vm_seal_cap),
        f("VM35", "validate_manifest", "validates every seal_pending item",
          "    for index, item in enumerate(seal_pending):\n"
          "        _text(item, f\"finalize.seal_pending[{index}]\", 128)\n",
          "    for index, item in enumerate(seal_pending):\n"
          "        pass\n",
          reject_manifest(
              lambda value: value["finalize"].__setitem__("seal_pending", [1]),
              "finalize.seal_pending[0] must be a string")),
        f("VM36", "validate_manifest", "applies completion checks only to complete runs",
          complete_guard,
          complete_guard.replace(
              'if state == "complete" and', "if True and"),
          p_vm_complete_state_gate),
        f("VM37", "validate_manifest", "complete runs require fetch_finished",
          complete_guard,
          complete_guard.replace('not data["fetch_finished"] or ', ""),
          p_vm_complete_fetch),
        f("VM38", "validate_manifest", "complete runs require every ticker verified",
          complete_guard,
          "    if state == \"complete\" and (not data[\"fetch_finished\"]):\n"
          "        raise ManifestError(\"complete run has unfinished work\")\n",
          p_vm_complete_verified),
    )


def encode_fences() -> tuple[Fence, ...]:
    return (
        f("E1", "_encode", "validates before serialization",
          "def _encode(data):\n    validate_manifest(data)\n",
          "def _encode(data):\n    pass\n",
          lambda m: raises_exact(
              lambda: m._encode({"unsafe": True}), m.ManifestError,
              "manifest root fields do not match schema 1")),
        f("E2", "_encode", "emits sorted compact canonical JSON bytes",
          '    raw = json.dumps(data, sort_keys=True, separators=(",", ":")).encode("utf-8")\n',
          '    raw = json.dumps(data).encode("utf-8")\n',
          p_encode_canonical),
        f("E3", "_encode", "rejects schema-valid manifests over 4 MiB",
          "    if len(raw) > MAX_BYTES:\n"
          "        raise ManifestError(\"manifest exceeds size limit\")\n",
          "    if len(raw) > 64 * 1024 * 1024:\n"
          "        raise ManifestError(\"manifest exceeds size limit\")\n",
          p_encode_cap),
    )


def p_pending_dedupe(module: types.ModuleType) -> bool:
    value = manifest(finalize={
        "at": None, "reason": None, "seal_pending": ["AAA 1m"]
    })
    return module.pending_selections(value) == [("AAA", "1m")]


def p_pending_invalid_seal(module: types.ModuleType) -> bool:
    value = manifest(finalize={
        "at": None, "reason": None,
        "seal_pending": ["bad", "!!! 1m", "CCC 1m"],
    })
    return module.pending_selections(value) == [("AAA", "1m")]


def p_pending_no_prior(module: types.ModuleType) -> bool:
    item = built_record(
        interval_map={
            "1m-pre": series(state="complete", xval=True, gaps=True),
            "1m": series(state="complete", xval=True, gaps=True),
        },
        missing=["earliest"], earliest=False,
    )
    value = manifest(
        tickers={"AAA": item},
        finalize={"at": None, "reason": None,
                  "seal_pending": ["AAA 1m-pre"]},
    )
    return module.pending_selections(value) == [("AAA", "1m-pre")]


def pending_fences() -> tuple[Fence, ...]:
    earliest_condition = (
        "        if (include_earliest_probe and record[\"state\"] == \"built\"\n"
        "                and \"earliest\" in record[\"missing\"]\n"
        "                and not any(item[0] == ticker for item in ordered)):\n"
    )
    return (
        f("P1", "pending_selections", "validates before selecting work",
          "def pending_selections(data, *, include_earliest_probe=True):\n"
          "    validate_manifest(data)\n",
          "def pending_selections(data, *, include_earliest_probe=True):\n"
          "    pass\n",
          lambda m: raises_exact(
              lambda: m.pending_selections({"bad": True}), m.ManifestError,
              "manifest root fields do not match schema 1")),
        f("P2", "pending_selections", "deduplicates seal and series work",
          "    def add(ticker, interval):\n"
          "        item = (ticker, interval)\n"
          "        if item not in seen:\n",
          "    def add(ticker, interval):\n"
          "        item = (ticker, interval)\n"
          "        if True:\n",
          p_pending_dedupe),
        f("P3", "pending_selections", "accepts only two-token seal entries",
          "        if len(parts) == 2:\n", "        if True:\n",
          p_pending_invalid_seal),
        f("P4", "pending_selections", "skips malformed canonical seal entries",
          "            except ManifestError:\n"
          "                continue\n"
          "            if ticker in data[\"tickers\"]",
          "            except ManifestError:\n"
          "                raise\n"
          "            if ticker in data[\"tickers\"]",
          p_pending_invalid_seal),
        f("P5", "pending_selections", "keeps seal work inside manifest membership",
          "            if ticker in data[\"tickers\"] and interval in data[\"tickers\"][ticker][\"series\"]:\n"
          "                add(ticker, interval)\n",
          "            if True:\n"
          "                add(ticker, interval)\n",
          p_pending_invalid_seal),
        f("P6", "pending_selections", "selects every incomplete series",
          "            if series[\"state\"] != \"complete\":\n"
          "                add(ticker, interval)\n",
          "            if False:\n"
          "                add(ticker, interval)\n",
          lambda m: m.pending_selections(manifest()) == [("AAA", "1m")]),
        f("P7", "pending_selections", "honors include_earliest_probe False",
          earliest_condition,
          earliest_condition.replace("if (include_earliest_probe and", "if (True and"),
          lambda m: m.pending_selections(
              manifest(tickers={"AAA": built_record()}),
              include_earliest_probe=False,
          ) == []),
        f("P8", "pending_selections", "limits earliest probes to built tickers",
          earliest_condition,
          earliest_condition.replace(
              'record["state"] == "built"', 'record["state"] == "verified"'),
          lambda m: m.pending_selections(
              manifest(tickers={"AAA": built_record()})
          ) == [("AAA", "1m")]),
        f("P9", "pending_selections", "requires actual earliest debt",
          earliest_condition,
          earliest_condition.replace(
              '"earliest" in record["missing"]', "True"),
          lambda m: m.pending_selections(manifest(tickers={
              "AAA": built_record(
                  interval_map={
                      "1m": series(state="complete", xval=False, gaps=True)
                  },
                  missing=["xval"], earliest=True,
              )
          })) == []),
        f("P10", "pending_selections", "does not add a second item for a sealed ticker",
          earliest_condition,
          earliest_condition.replace(
              "and not any(item[0] == ticker for item in ordered)", "and True"),
          p_pending_no_prior),
        f("P11", "pending_selections", "prefers RTH for earliest and falls back deterministically",
          "            interval = next((iv for iv in record[\"series\"] if _is_rth(iv)),\n"
          "                            next(iter(record[\"series\"])))\n",
          "            interval = next(iter(record[\"series\"]))\n",
          p_pending_preference),
    )


def p_summary_pending(module: types.ModuleType) -> bool:
    value = manifest(tickers={
        "AAA": record(),
        "BBB": record(series_map={"1m": series(state="building")},
                      state="building"),
    })
    return module.summary(value)["pending"] == 2


def p_summary_remaining(module: types.ModuleType) -> bool:
    value = manifest(tickers={"AAA": built_record()})
    result = module.summary(value)
    return result["remaining_series"] == 0 and result["built_unverified"] == 1


def summary_fences() -> tuple[Fence, ...]:
    return (
        f("SU1", "summary", "validates before reporting",
          "def summary(data):\n    validate_manifest(data)\n",
          "def summary(data):\n    pass\n",
          lambda m: raises_exact(
              lambda: m.summary({"bad": True}), m.ManifestError,
              "manifest root fields do not match schema 1")),
        f("SU2", "summary", "preserves run_id and state under their named keys",
          '        "run_id": data["run_id"],\n'
          '        "state": data["state"],\n',
          '        "run_id": data["state"],\n'
          '        "state": data["run_id"],\n',
          lambda m: (
              lambda value: value["run_id"] == "addstock-synthetic"
              and value["state"] == "active"
          )(m.summary(manifest()))),
        f("SU3", "summary", "counts pending and building tickers together",
          '    pending = sum(record["state"] in {"pending", "building"}\n',
          '    pending = sum(record["state"] == "pending"\n',
          p_summary_pending),
        f("SU4", "summary", "counts built verification debt",
          '    debt = sum(record["state"] == "built"\n',
          '    debt = sum(record["state"] == "pending"\n',
          p_summary_remaining),
        f("SU5", "summary", "counts verified tickers",
          '    verified = sum(record["state"] == "verified"\n',
          '    verified = sum(record["state"] == "built"\n',
          lambda m: m.summary(manifest(tickers={
              "AAA": verified_record()
          }))["verified"] == 1),
        f("SU6", "summary", "reports total ticker cardinality",
          '        "total": len(data["tickers"]),\n',
          '        "total": 0,\n',
          lambda m: m.summary(manifest(tickers={
              "AAA": record(), "BBB": record()
          }))["total"] == 2),
        f("SU7", "summary", "excludes earliest-only probes from remaining series",
          "            data, include_earliest_probe=False)),\n",
          "            data, include_earliest_probe=True)),\n",
          p_summary_remaining),
    )


def redundancy_cases() -> tuple[RedundantClause, ...]:
    return (
        RedundantClause(
            "RR1", "timestamp Z replacement is redundant on Python 3.14",
            mu('value.replace("Z", "+00:00")', "value"),
            lambda m: m._timestamp("2026-08-04T12:00:00Z", "at")
            == "2026-08-04T12:00:00Z",
        ),
        RedundantClause(
            "RR2", "canonical_ticker already coerces ticker through str",
            mu(
                "def _canon_selection(ticker, interval):\n"
                "    try:\n"
                "        ticker = ss.canonical_ticker(str(ticker))\n",
                "def _canon_selection(ticker, interval):\n"
                "    try:\n"
                "        ticker = ss.canonical_ticker(ticker)\n",
            ),
            lambda m: m._canon_selection(TextLike("aaa"), "1m")
            == ("AAA", "1m"),
        ),
        RedundantClause(
            "RR3", "anchored INTERVAL_RE makes fullmatch and search equivalent",
            mu("ss.INTERVAL_RE.fullmatch(interval)", "ss.INTERVAL_RE.search(interval)"),
            lambda m: (
                m._canon_selection("AAA", "1m") == ("AAA", "1m")
                and raises_exact(
                    lambda: m._canon_selection("AAA", "xx1mxx"),
                    m.ManifestError, "invalid interval: 'xx1mxx'",
                )
            ),
        ),
        RedundantClause(
            "RR4", "date.isoformat is redundant before str(date)",
            mu(
                "        since = since.date().isoformat() if isinstance(since, datetime) \\\n"
                "            else since.isoformat()\n",
                "        since = since.date().isoformat() if isinstance(since, datetime) \\\n"
                "            else since\n",
            ),
            lambda m: m._normalize_params(
                {"since": date(2026, 8, 4)}, [("AAA", "1m")]
            )["since"] == "2026-08-04",
        ),
    )


EXPECTED_FENCE_COUNTS = {
    "_valid_sha256": 3,
    "_utc_text": 7,
    "new_run_id": 3,
    "_text": 5,
    "_timestamp": 5,
    "_canon_selection": 6,
    "_is_rth": 2,
    "_series_record": 3,
    "_normalize_params": 20,
    "_validate_series_map": 11,
    "_recompute_ticker": 9,
    "validate_manifest": 38,
    "_encode": 3,
    "pending_selections": 11,
    "summary": 7,
}


def known_defects(module: types.ModuleType) -> tuple[tuple[str, str, bool], ...]:
    value = manifest()
    value["schema"] = True
    return ((
        "KD1", "bool True is accepted as schema version 1",
        accepts(lambda: module.validate_manifest(value)),
    ),)


def run() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    kit = CheckKit()
    kit.section("Row 85 S2 shipped-source custody and inventory")
    before = source_bytes()
    before_hash = digest(before)
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
    kit.check("HARNESS-INVENTORY final code-derived fence count is 133",
              len(cases) == 133, str(len(cases)))
    kit.check("HARNESS-INVENTORY per-function counts match derivation",
              dict(actual_counts) == EXPECTED_FENCE_COUNTS,
              repr(dict(actual_counts)))
    kit.check("HARNESS-INVENTORY fence IDs are unique",
              len(set(ids)) == len(ids), repr(ids))
    kit.check("HARNESS-INVENTORY four redundant clauses are explicit",
              len(redundancies) == 4, str(len(redundancies)))
    kit.check("HARNESS-INVENTORY redundancy IDs are unique",
              len(set(redundant_ids)) == len(redundant_ids),
              repr(redundant_ids))
    kit.check("CONTAINMENT fixture ticker vocabulary excludes protected names",
              ALL_SYNTHETIC_TICKERS.isdisjoint(FORBIDDEN_TICKERS),
              repr(sorted(ALL_SYNTHETIC_TICKERS & FORBIDDEN_TICKERS)))

    kit.section("One named baseline check per manifest-core fence")
    for case in cases:
        passed, detail = evaluate(case.probe, baseline)
        kit.check(
            f"FENCE {case.fence_id} {case.function} {case.description}",
            passed, detail,
        )

    kit.section("One compiling in-memory mutation killed per fence")
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

    kit.section("Behavior-preserving clause deletions")
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
        first_failure = ""
        all_survived = True
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
            f"{redundant.description}", proof, detail,
        )

    kit.section("Pinned latent defects; production engine unchanged")
    for defect_id, description, present in known_defects(baseline):
        kit.check(f"KNOWN-DEFECT {defect_id} {description}", present)

    after_hash = digest(source_bytes())
    kit.check("SOURCE-CUSTODY production bytes unchanged during mutations",
              after_hash == before_hash, after_hash)
    kit.check(
        "SOURCE-CUSTODY fenced-function digests unchanged during mutations",
        function_segment_sha256(normalized_source(source_bytes()), owners)
        == segment_hashes,
        repr(function_segment_sha256(
            normalized_source(source_bytes()), owners)),
    )
    return kit.finish()


if __name__ == "__main__":
    raise SystemExit(run())
