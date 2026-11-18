"""Offline mutation reference for the pure Fix Data validator slice.

The harness executes the shipped ``fix_data_pipeline.py`` source from memory.
It never imports the production module by name, edits it, invokes ``run()``,
opens a bank, acquires the operation gate, or constructs an adapter.  Each
fence has one baseline probe and one small, compiling source mutation that its
own probe must kill.  Clauses found redundant against the shipped constants
are deleted separately and must survive the complete fence fixture corpus.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
from pathlib import Path
import sys
import types
from typing import Callable


ENGINE_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = ENGINE_ROOT.parent
TARGET = ENGINE_ROOT / "fix_data_pipeline.py"
EXPECTED_SOURCE_SHA256 = (
    "5a0fd333b364b11b40aea88f85c24e42fd83cba6725dc8c5ea6870cd4bb55306"
)

if str(ENGINE_ROOT) not in sys.path:
    sys.path.insert(0, str(ENGINE_ROOT))

from check_kit import CheckKit  # noqa: E402


TEST_ONLY = True


class HarnessError(RuntimeError):
    """The shipped source no longer matches a mutation anchor."""


@dataclass(frozen=True)
class Mutation:
    old: str
    new: str

    def apply(self, source: str) -> str:
        count = source.count(self.old)
        if count != 1:
            raise HarnessError(
                f"mutation anchor count is {count}, expected 1: "
                f"{self.old[:100]!r}"
            )
        return source.replace(self.old, self.new, 1)


Probe = Callable[[types.ModuleType], bool]


@dataclass(frozen=True)
class Fence:
    fence_id: str
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


def load_module(source: str) -> types.ModuleType:
    module = types.ModuleType("_fixdata_validator_fence_subject")
    module.__file__ = str(TARGET)
    module.__package__ = ""
    code = compile(source, str(TARGET), "exec")
    exec(code, module.__dict__)
    return module


def evaluate(probe: Probe, module: types.ModuleType) -> tuple[bool, str]:
    try:
        result = probe(module)
    except BaseException as exc:  # the exact exception behavior is the subject
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


def row(ticker: object = "ABC", kind: object = "stock_ibkr",
        day: object = "2026-08-03") -> dict[str, object]:
    return {"ticker": ticker, "kind": kind, "day": day}


def normalized(*, reason: str = "hard_nonfinite",
               reasons: list[str] | None = None) -> dict[str, object]:
    return {
        "day": "2026-08-03",
        "reason": reason,
        "reasons": ["hard_nonfinite"] if reasons is None else reasons,
    }


def correction(**updates: object) -> dict[str, object]:
    value: dict[str, object] = {
        "version": 1,
        "type": "vol_value_refetch_reconcile",
        "day": "2026-08-03",
        "run": "row86",
        "source": "bank",
        "confirmed": "2026-08-03T12:00:00Z",
        "what_to_show": "OPTION_IMPLIED_VOLATILITY",
        "request_count": 1,
        "bars": 10,
        "old_value": None,
        "new_value": 0.25,
        "old_day_sha256": "a" * 64,
        "new_day_sha256": "b" * 64,
        "old_month_sha256": "c" * 64,
        "new_month_sha256": "d" * 64,
        "reason": "hard_nonfinite",
        "reasons": ["hard_nonfinite"],
    }
    value.update(updates)
    return value


class TextLike:
    """Non-str object that otherwise behaves like the supplied text."""

    def __init__(self, value: str):
        self.value = value

    def __bool__(self) -> bool:
        return bool(self.value)

    def __len__(self) -> int:
        return len(self.value)

    def __contains__(self, item: object) -> bool:
        return item in self.value

    def __iter__(self):
        return iter(self.value)

    def __eq__(self, other: object) -> bool:
        return self.value == other

    def __str__(self) -> str:
        return self.value


class ReasonsLike:
    """Non-list sequence equal to the corresponding canonical list."""

    def __init__(self, values: list[str]):
        self.values = tuple(values)

    def __bool__(self) -> bool:
        return bool(self.values)

    def __len__(self) -> int:
        return len(self.values)

    def __iter__(self):
        return iter(self.values)

    def __eq__(self, other: object) -> bool:
        return list(self.values) == other

    def __ne__(self, other: object) -> bool:
        return not self == other


class InfiniteRows:
    def __init__(self):
        self.count = 0

    def __iter__(self):
        return self

    def __next__(self):
        self.count += 1
        return row(ticker=f"T{self.count}")


class HostileCorrection(dict):
    """A dict subclass whose inherited data are valid but get() lies."""

    def get(self, key, default=None):
        if key == "version":
            return 2
        return super().get(key, default)


def with_item_cap(module: types.ModuleType, cap: int,
                  call: Callable[[], bool]) -> bool:
    previous = module.MAX_RECONCILE_ITEMS
    module.MAX_RECONCILE_ITEMS = cap
    try:
        return call()
    finally:
        module.MAX_RECONCILE_ITEMS = previous


def p_t6(module: types.ModuleType) -> bool:
    original = row()
    result = module._reconcile_items([original])
    result[0]["ticker"] = "MUTATED"
    return result[0] is not original and original["ticker"] == "ABC"


def p_t7(module: types.ModuleType) -> bool:
    stream = InfiniteRows()

    def run() -> bool:
        rejected = raises_exact(
            lambda: module._reconcile_items(stream), ValueError,
            "reconcile plan exceeds the bounded item limit",
        )
        return rejected and stream.count == 4

    return with_item_cap(module, 3, run)


def p_t8(module: types.ModuleType) -> bool:
    return with_item_cap(
        module, 3,
        lambda: raises_exact(
            lambda: module._reconcile_items([
                row(ticker="A"), row(ticker="B"), row(ticker="C"),
                row(ticker="D"),
            ]),
            ValueError, "reconcile plan exceeds the bounded item limit",
        ),
    )


def p_t9(module: types.ModuleType) -> bool:
    try:
        module._reconcile_items([[("broken", 1, 2)]])
    except BaseException as exc:
        return (
            type(exc) is TypeError
            and str(exc) == "reconcile plan must be an iterable of objects"
            and type(exc.__cause__) is ValueError
        )
    return False


def p_c3(module: types.ModuleType) -> bool:
    return (
        module._commit_text("x" * 320, "text") == "x" * 320
        and raises_exact(
            lambda: module._commit_text("x" * 321, "text"), ValueError,
            "reconcile text is invalid",
        )
        and module._commit_text("x" * 64, "text", limit=64) == "x" * 64
        and raises_exact(
            lambda: module._commit_text("x" * 65, "text", limit=64),
            ValueError, "reconcile text is invalid",
        )
    )


def p_c4(module: types.ModuleType) -> bool:
    return all(
        raises_exact(
            lambda bad=bad: module._commit_text(bad, "text"),
            ValueError, "reconcile text is invalid",
        )
        for bad in ("a\rb", "a\nb", "a\x00b")
    )


def p_n5(module: types.ModuleType) -> bool:
    return all(
        raises_exact(
            lambda value=value: module._commit_number(value, "number"),
            ValueError, "reconcile number must be finite",
        )
        for value in (float("inf"), float("nan"))
    )


def p_r6(module: types.ModuleType) -> bool:
    ordered = sorted(("hard_nonfinite", "jump_up"))
    unsorted = list(reversed(ordered))
    duplicated = [ordered[0], ordered[0]]
    return (
        raises_exact(
            lambda: module._commit_reasons(",".join(unsorted), unsorted),
            ValueError, "reconcile atomic reason fields are invalid",
        )
        and raises_exact(
            lambda: module._commit_reasons(",".join(duplicated), duplicated),
            ValueError, "reconcile atomic reason fields are invalid",
        )
    )


def p_r9(module: types.ModuleType) -> bool:
    reasons = ["hard_nonfinite"]
    reason, detached = module._commit_reasons("hard_nonfinite", reasons)
    return reason == "hard_nonfinite" and detached == reasons and detached is not reasons


def p_x2(module: types.ModuleType) -> bool:
    missing = correction()
    missing.pop("old_value")
    extra = correction(extra_field="no")
    return all(
        raises_exact(
            lambda item=item: module._commit_correction(
                item, normalized=normalized()),
            ValueError, "reconcile correction evidence fields are invalid",
        )
        for item in (missing, extra)
    )


def p_x8(module: types.ModuleType) -> bool:
    return (
        raises_exact(
            lambda: module._commit_correction(
                correction(bars=0), normalized=normalized()),
            ValueError, "reconcile correction bar count is invalid",
        )
        and accepts(lambda: module._commit_correction(
            correction(bars=1), normalized=normalized()))
        and accepts(lambda: module._commit_correction(
            correction(bars=2_000_000), normalized=normalized()))
        and raises_exact(
            lambda: module._commit_correction(
                correction(bars=2_000_001), normalized=normalized()),
            ValueError, "reconcile correction bar count is invalid",
        )
    )


def p_x18(module: types.ModuleType) -> bool:
    return all(
        raises_exact(
            lambda value=value: module._commit_correction(
                correction(new_value=value), normalized=normalized()),
            ValueError, "reconcile correction new_value must be finite",
        )
        for value in (None, float("inf"))
    )


def p_x26(module: types.ModuleType) -> bool:
    result = module._commit_correction(correction(), normalized=normalized())
    return set(result) == set(correction()) and result["bars"] == 10


def p_x27(module: types.ModuleType) -> bool:
    return accepts(lambda: module._commit_correction(
        HostileCorrection(correction()), normalized=normalized()))


def fences() -> tuple[Fence, ...]:
    identity_guard = (
        "    if not isinstance(row, dict):\n"
        "        raise TypeError(\"reconcile plan rows must be objects\")\n"
    )
    incomplete_guard = (
        "    if not ticker or not kind or not day:\n"
        "        raise ValueError(\"reconcile plan row identity is incomplete\")\n"
    )
    valid_failure_keys = {
        "status", "ticker", "kind", "kind_token", "day",
        "request_count", "queue_resolved", "error",
        "request_count_unknown",
    }
    entries = (
        Fence("I1", "identity rejects non-dict with exact TypeError",
              mu(identity_guard, identity_guard.replace("if not isinstance(row, dict):", "if False:")),
              lambda m: raises_exact(
                  lambda: m._reconcile_identity([]), TypeError,
                  "reconcile plan rows must be objects")),
        Fence("I2", "ticker str coercion",
              mu('    ticker = str(row.get("ticker") or "").strip().upper()\n',
                 '    ticker = (row.get("ticker") or "").strip().upper()\n'),
              lambda m: m._reconcile_identity(row(ticker=7))[0] == "7"),
        Fence("I3", "ticker None defaults to empty before validation",
              mu('    ticker = str(row.get("ticker") or "").strip().upper()\n',
                 '    ticker = str(row.get("ticker")).strip().upper()\n'),
              lambda m: raises_exact(
                  lambda: m._reconcile_identity(row(ticker=None)), ValueError,
                  "reconcile plan row identity is incomplete")),
        Fence("I4", "ticker strips whitespace",
              mu('    ticker = str(row.get("ticker") or "").strip().upper()\n',
                 '    ticker = str(row.get("ticker") or "").upper()\n'),
              lambda m: m._reconcile_identity(row(ticker=" abc "))[0] == "ABC"),
        Fence("I5", "ticker uppercases",
              mu('    ticker = str(row.get("ticker") or "").strip().upper()\n',
                 '    ticker = str(row.get("ticker") or "").strip()\n'),
              lambda m: m._reconcile_identity(row(ticker="abc"))[0] == "ABC"),
        Fence("I6", "kind_token falls back to kind",
              mu('    kind = str(row.get("kind_token") or row.get("kind") or "").strip()\n',
                 '    kind = str(row.get("kind_token") or "").strip()\n'),
              lambda m: m._reconcile_identity(row())[1] == "stock_ibkr"),
        Fence("I7", "kind None defaults to empty before validation",
              mu('    kind = str(row.get("kind_token") or row.get("kind") or "").strip()\n',
                 '    kind = str(row.get("kind_token") or row.get("kind")).strip()\n'),
              lambda m: raises_exact(
                  lambda: m._reconcile_identity(row(kind=None)), ValueError,
                  "reconcile plan row identity is incomplete")),
        Fence("I8", "kind str coercion",
              mu('    kind = str(row.get("kind_token") or row.get("kind") or "").strip()\n',
                 '    kind = (row.get("kind_token") or row.get("kind") or "").strip()\n'),
              lambda m: m._reconcile_identity(row(kind=7))[1] == "7"),
        Fence("I9", "kind strips whitespace",
              mu('    kind = str(row.get("kind_token") or row.get("kind") or "").strip()\n',
                 '    kind = str(row.get("kind_token") or row.get("kind") or "")\n'),
              lambda m: m._reconcile_identity(row(kind=" stock_ibkr "))[1] == "stock_ibkr"),
        Fence("I10", "day str coercion",
              mu('    day = str(row.get("day") or "").strip()\n',
                 '    day = (row.get("day") or "").strip()\n'),
              lambda m: m._reconcile_identity(row(day=20260803))[2] == "20260803"),
        Fence("I11", "day None defaults to empty before validation",
              mu('    day = str(row.get("day") or "").strip()\n',
                 '    day = str(row.get("day")).strip()\n'),
              lambda m: raises_exact(
                  lambda: m._reconcile_identity(row(day=None)), ValueError,
                  "reconcile plan row identity is incomplete")),
        Fence("I12", "day strips whitespace",
              mu('    day = str(row.get("day") or "").strip()\n',
                 '    day = str(row.get("day") or "")\n'),
              lambda m: m._reconcile_identity(row(day=" 2026-08-03 "))[2] == "2026-08-03"),
        Fence("I13", "empty ticker rejected with exact ValueError",
              mu(incomplete_guard, incomplete_guard.replace("not ticker or ", "")),
              lambda m: raises_exact(
                  lambda: m._reconcile_identity(row(ticker="")), ValueError,
                  "reconcile plan row identity is incomplete")),
        Fence("I14", "empty kind rejected with exact ValueError",
              mu(incomplete_guard, incomplete_guard.replace("not kind or ", "")),
              lambda m: raises_exact(
                  lambda: m._reconcile_identity(row(kind="")), ValueError,
                  "reconcile plan row identity is incomplete")),
        Fence("I15", "empty day rejected with exact ValueError",
              mu(incomplete_guard, incomplete_guard.replace(" or not day", "")),
              lambda m: raises_exact(
                  lambda: m._reconcile_identity(row(day="")), ValueError,
                  "reconcile plan row identity is incomplete")),
        Fence("I16", "identity return tuple shape and order",
              mu("    return ticker, kind, day\n\n\ndef _reconcile_debt",
                 "    return kind, ticker, day\n\n\ndef _reconcile_debt"),
              lambda m: m._reconcile_identity(row()) == (
                  "ABC", "stock_ibkr", "2026-08-03")),

        Fence("D1", "debt delegates identity validation and normalization",
              mu("def _reconcile_debt(row, reason):\n"
                 "    ticker, kind, day = _reconcile_identity(row)\n",
                 "def _reconcile_debt(row, reason):\n"
                 "    ticker, kind, day = row['ticker'], row['kind'], row['day']\n"),
              lambda m: m._reconcile_debt(
                  row(ticker=" abc "), "why")[:3]
              == ("ABC", "stock_ibkr", "2026-08-03")),
        Fence("D2", "debt emits reconcile stage tag",
              mu('    return ticker, kind, day, "reconcile", str(reason)\n',
                 '    return ticker, kind, day, "repair", str(reason)\n'),
              lambda m: m._reconcile_debt(row(), "why")[3] == "reconcile"),
        Fence("D3", "debt coerces reason to str",
              mu('    return ticker, kind, day, "reconcile", str(reason)\n',
                 '    return ticker, kind, day, "reconcile", reason\n'),
              lambda m: m._reconcile_debt(row(), 7)[4] == "7"),
        Fence("D4", "debt return tuple shape and order",
              mu('    return ticker, kind, day, "reconcile", str(reason)\n',
                 '    return ticker, day, kind, "reconcile", str(reason)\n'),
              lambda m: m._reconcile_debt(row(), "why") == (
                  "ABC", "stock_ibkr", "2026-08-03", "reconcile", "why")),

        Fence("T1", "items maps None to an empty list",
              mu("    if value is None:\n        return []\n",
                 "    if False:\n        return []\n"),
              lambda m: m._reconcile_items(None) == []),
        Fence("T2", "items unwraps the items key",
              mu('        if "items" in value:\n            value = value["items"]\n',
                 '        if "__items" in value:\n            value = value["__items"]\n'),
              lambda m: m._reconcile_items({"items": [row()]}) == [row()]),
        Fence("T3", "items unwraps the rows key",
              mu('        elif "rows" in value:\n            value = value["rows"]\n',
                 '        elif "__rows" in value:\n            value = value["__rows"]\n'),
              lambda m: m._reconcile_items({"rows": [row()]}) == [row()]),
        Fence("T4", "items rejects dict without items or rows",
              mu('        else:\n            raise TypeError("reconcile plan object must contain items or rows")\n',
                 '        else:\n            return []\n'),
              lambda m: raises_exact(
                  lambda: m._reconcile_items({}), TypeError,
                  "reconcile plan object must contain items or rows")),
        Fence("T5", "items rejects str and bytes before iteration",
              mu('    if isinstance(value, (str, bytes)):\n'
                 '        raise TypeError("reconcile plan must be an iterable of objects")\n',
                 '    if False:\n'
                 '        raise TypeError("reconcile plan must be an iterable of objects")\n'),
              lambda m: all(raises_exact(
                  lambda value=value: m._reconcile_items(value), TypeError,
                  "reconcile plan must be an iterable of objects")
                  for value in ("", b""))),
        Fence("T6", "items detaches each row dict",
              mu("        rows = [dict(row) for row in islice(\n",
                 "        rows = [row for row in islice(\n"), p_t6),
        Fence("T7", "items bounds generator consumption with islice",
              mu("            iter(value), MAX_RECONCILE_ITEMS + 1)]\n",
                 "            iter(value), MAX_RECONCILE_ITEMS + 2)]\n"), p_t7),
        Fence("T8", "items rejects plans over the configured bound",
              mu("    if len(rows) > MAX_RECONCILE_ITEMS:\n"
                 "        raise ValueError(\"reconcile plan exceeds the bounded item limit\")\n",
                 "    if False:\n"
                 "        raise ValueError(\"reconcile plan exceeds the bounded item limit\")\n"), p_t8),
        Fence("T9", "items wraps inner ValueError as exact TypeError",
              mu('        raise TypeError("reconcile plan must be an iterable of objects") from exc\n',
                 "        raise exc\n"), p_t9),
        Fence("T10", "items preserves ticker None as unscoped",
              mu("    expected_ticker = (None if ticker is None\n"
                 "                       else str(ticker).strip().upper())\n",
                 "    expected_ticker = str(ticker).strip().upper()\n"),
              lambda m: m._reconcile_items([row()], ticker=None) == [row()]),
        Fence("T11", "items coerces expected ticker to str",
              mu("                       else str(ticker).strip().upper())\n",
                 "                       else ticker.strip().upper())\n"),
              lambda m: m._reconcile_items([row(ticker="7")], ticker=7)
              == [row(ticker="7")]),
        Fence("T12", "items strips expected ticker",
              mu("                       else str(ticker).strip().upper())\n",
                 "                       else str(ticker).upper())\n"),
              lambda m: m._reconcile_items([row()], ticker=" ABC ") == [row()]),
        Fence("T13", "items uppercases expected ticker",
              mu("                       else str(ticker).strip().upper())\n",
                 "                       else str(ticker).strip())\n"),
              lambda m: m._reconcile_items([row()], ticker="abc") == [row()]),
        Fence("T14", "items rejects ticker scope escape",
              mu("        if expected_ticker is not None and key[0] != expected_ticker:\n"
                 "            raise ValueError(\n"
                 "                \"post-repair reconcile plan escaped its ticker scope\")\n",
                 "        if False:\n"
                 "            raise ValueError(\n"
                 "                \"post-repair reconcile plan escaped its ticker scope\")\n"),
              lambda m: raises_exact(
                  lambda: m._reconcile_items([row(ticker="XYZ")], ticker="ABC"),
                  ValueError, "post-repair reconcile plan escaped its ticker scope")),
        Fence("T15", "items rejects duplicate identities",
              mu("        if key in seen:\n"
                 "            raise ValueError(f\"duplicate reconcile plan row: {' '.join(key)}\")\n",
                 "        if False:\n"
                 "            raise ValueError(f\"duplicate reconcile plan row: {' '.join(key)}\")\n"),
              lambda m: raises_exact(
                  lambda: m._reconcile_items([row(), row()]), ValueError,
                  "duplicate reconcile plan row: ABC stock_ibkr 2026-08-03")),

        Fence("F1", "failure delegates identity validation",
              mu("def _reconcile_failure(row, exc, *, ambiguous=True):\n"
                 "    ticker, kind, day = _reconcile_identity(row)\n",
                 "def _reconcile_failure(row, exc, *, ambiguous=True):\n"
                 "    ticker, kind, day = row['ticker'], row['kind'], row['day']\n"),
              lambda m: raises_exact(
                  lambda: m._reconcile_failure(row(ticker=""), RuntimeError()),
                  ValueError, "reconcile plan row identity is incomplete")),
        Fence("F2", "failure maps both ambiguous status arms",
              mu('        "status": "ambiguous" if ambiguous else "unresolved",\n',
                 '        "status": "ambiguous",\n'),
              lambda m: (
                  m._reconcile_failure(row(), RuntimeError(), ambiguous=True)["status"] == "ambiguous"
                  and m._reconcile_failure(row(), RuntimeError(), ambiguous=False)["status"] == "unresolved")),
        Fence("F3", "failure emits zero request count",
              mu('        "kind_token": kind, "day": day, "request_count": 0,\n',
                 '        "kind_token": kind, "day": day, "request_count": 1,\n'),
              lambda m: m._reconcile_failure(row(), RuntimeError())["request_count"] == 0),
        Fence("F4", "failure leaves queue unresolved",
              mu('        "queue_resolved": False, "error": _error(exc),\n',
                 '        "queue_resolved": True, "error": _error(exc),\n'),
              lambda m: m._reconcile_failure(row(), RuntimeError())["queue_resolved"] is False),
        Fence("F5", "failure formats exception through _error",
              mu('        "queue_resolved": False, "error": _error(exc),\n',
                 '        "queue_resolved": False, "error": str(exc),\n'),
              lambda m: m._reconcile_failure(
                  row(), RuntimeError("boom"))["error"] == "RuntimeError: boom"),
        Fence("F6", "failure emits request_count_unknown only when ambiguous",
              mu('    if ambiguous:\n        result["request_count_unknown"] = True\n',
                 '    if True:\n        result["request_count_unknown"] = True\n'),
              lambda m: (
                  "request_count_unknown" in m._reconcile_failure(
                      row(), RuntimeError(), ambiguous=True)
                  and "request_count_unknown" not in m._reconcile_failure(
                      row(), RuntimeError(), ambiguous=False))),
        Fence("F7", "failure output schema mirrors kind_token",
              mu('        "kind_token": kind, "day": day, "request_count": 0,\n',
                 '        "kind_token": None, "day": day, "request_count": 0,\n'),
              lambda m: (
                  set(m._reconcile_failure(row(), RuntimeError())) == valid_failure_keys
                  and m._reconcile_failure(row(), RuntimeError())["kind_token"]
                  == "stock_ibkr")),

        Fence("C1", "commit text rejects non-str with ValueError",
              mu("def _commit_text(value, label, *, limit=320):\n"
                 "    if (not isinstance(value, str) or not value\n",
                 "def _commit_text(value, label, *, limit=320):\n"
                 "    if (False or not value\n"),
              lambda m: raises_exact(
                  lambda: m._commit_text(TextLike("ok"), "text"), ValueError,
                  "reconcile text is invalid")),
        Fence("C2", "commit text rejects empty string",
              mu("def _commit_text(value, label, *, limit=320):\n"
                 "    if (not isinstance(value, str) or not value\n",
                 "def _commit_text(value, label, *, limit=320):\n"
                 "    if (not isinstance(value, str) or False\n"),
              lambda m: raises_exact(
                  lambda: m._commit_text("", "text"), ValueError,
                  "reconcile text is invalid")),
        Fence("C3", "commit text enforces default and call-site lengths",
              mu("            or len(value) > limit or any(c in value for c in \"\\r\\n\\x00\")):\n",
                 "            or False or any(c in value for c in \"\\r\\n\\x00\")):\n"), p_c3),
        Fence("C4", "commit text rejects CR LF and NUL",
              mu("            or len(value) > limit or any(c in value for c in \"\\r\\n\\x00\")):\n",
                 "            or len(value) > limit or False):\n"), p_c4),

        Fence("S1", "commit SHA nullable guard accepts None when enabled",
              mu("def _commit_sha(value, label, *, nullable=False):\n"
                 "    if value is None and nullable:\n        return None\n",
                 "def _commit_sha(value, label, *, nullable=False):\n"
                 "    if False:\n        return None\n"),
              lambda m: m._commit_sha(None, "sha", nullable=True) is None),
        Fence("S2", "commit SHA restricts None to nullable calls",
              mu("def _commit_sha(value, label, *, nullable=False):\n"
                 "    if value is None and nullable:\n        return None\n",
                 "def _commit_sha(value, label, *, nullable=False):\n"
                 "    if value is None:\n        return None\n"),
              lambda m: raises_exact(
                  lambda: m._commit_sha(None, "sha"), ValueError,
                  "reconcile sha is invalid")),
        Fence("S3", "commit SHA rejects non-str with ValueError",
              mu("def _commit_sha(value, label, *, nullable=False):\n"
                 "    if value is None and nullable:\n"
                 "        return None\n"
                 "    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:\n",
                 "def _commit_sha(value, label, *, nullable=False):\n"
                 "    if value is None and nullable:\n"
                 "        return None\n"
                 "    if _SHA256_RE.fullmatch(str(value)) is None:\n"),
              lambda m: raises_exact(
                  lambda: m._commit_sha(TextLike("a" * 64), "sha"),
                  ValueError, "reconcile sha is invalid")),
        Fence("S4", "commit SHA requires a full 64-hex match",
              mu("def _commit_sha(value, label, *, nullable=False):\n"
                 "    if value is None and nullable:\n"
                 "        return None\n"
                 "    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:\n",
                 "def _commit_sha(value, label, *, nullable=False):\n"
                 "    if value is None and nullable:\n"
                 "        return None\n"
                 "    if not isinstance(value, str) or _SHA256_RE.search(value) is None:\n"),
              lambda m: raises_exact(
                  lambda: m._commit_sha("prefix" + "a" * 64, "sha"),
                  ValueError, "reconcile sha is invalid")),

        Fence("N1", "commit number nullable guard accepts None when enabled",
              mu("def _commit_number(value, label, *, nullable=False):\n"
                 "    if value is None and nullable:\n        return None\n",
                 "def _commit_number(value, label, *, nullable=False):\n"
                 "    if False:\n        return None\n"),
              lambda m: m._commit_number(None, "number", nullable=True) is None),
        Fence("N2", "commit number restricts None to nullable calls",
              mu("def _commit_number(value, label, *, nullable=False):\n"
                 "    if value is None and nullable:\n        return None\n",
                 "def _commit_number(value, label, *, nullable=False):\n"
                 "    if value is None:\n        return None\n"),
              lambda m: raises_exact(
                  lambda: m._commit_number(None, "number"), ValueError,
                  "reconcile number must be finite")),
        Fence("N3", "commit number explicitly rejects bool",
              mu("def _commit_number(value, label, *, nullable=False):\n"
                 "    if value is None and nullable:\n"
                 "        return None\n"
                 "    if (isinstance(value, bool) or not isinstance(value, (int, float))\n",
                 "def _commit_number(value, label, *, nullable=False):\n"
                 "    if value is None and nullable:\n"
                 "        return None\n"
                 "    if (False or not isinstance(value, (int, float))\n"),
              lambda m: raises_exact(
                  lambda: m._commit_number(True, "number"), ValueError,
                  "reconcile number must be finite")),
        Fence("N4", "commit number rejects nonnumeric types",
              mu("def _commit_number(value, label, *, nullable=False):\n"
                 "    if value is None and nullable:\n"
                 "        return None\n"
                 "    if (isinstance(value, bool) or not isinstance(value, (int, float))\n",
                 "def _commit_number(value, label, *, nullable=False):\n"
                 "    if value is None and nullable:\n"
                 "        return None\n"
                 "    if (isinstance(value, bool) or False\n"),
              lambda m: raises_exact(
                  lambda: m._commit_number("1.25", "number"), ValueError,
                  "reconcile number must be finite")),
        Fence("N5", "commit number rejects infinity and NaN",
              mu("    if (isinstance(value, bool) or not isinstance(value, (int, float))\n"
                 "            or not math.isfinite(float(value))):\n",
                 "    if (isinstance(value, bool) or not isinstance(value, (int, float))\n"
                 "            or False):\n"), p_n5),
        Fence("N6", "commit number returns float for int input",
              mu("    return float(value)\n\n\ndef _commit_reasons",
                 "    return value\n\n\ndef _commit_reasons"),
              lambda m: type(m._commit_number(7, "number")) is float),

        Fence("R1", "commit reasons requires reason str type",
              mu("    if (not isinstance(reason, str) or not reason or len(reason) > 320\n",
                 "    if (False or not reason or len(reason) > 320\n"),
              lambda m: raises_exact(
                  lambda: m._commit_reasons(
                      TextLike("hard_nonfinite"), ["hard_nonfinite"]),
                  ValueError, "reconcile atomic reason fields are invalid")),
        Fence("R3", "commit reasons requires a list container",
              mu("            or not isinstance(reasons, list) or not reasons or len(reasons) > 16\n",
                 "            or False or not reasons or len(reasons) > 16\n"),
              lambda m: raises_exact(
                  lambda: m._commit_reasons(
                      "hard_nonfinite", ReasonsLike(["hard_nonfinite"])),
                  ValueError, "reconcile atomic reason fields are invalid")),
        Fence("R5", "commit reasons requires every item to be str",
              mu("def _commit_reasons(reason, reasons):\n"
                 "    if (not isinstance(reason, str) or not reason or len(reason) > 320\n"
                 "            or any(c in reason for c in \"\\r\\n\\x00\")\n"
                 "            or not isinstance(reasons, list) or not reasons or len(reasons) > 16\n"
                 "            or any(not isinstance(item, str) or not item\n",
                 "def _commit_reasons(reason, reasons):\n"
                 "    if (not isinstance(reason, str) or not reason or len(reason) > 320\n"
                 "            or any(c in reason for c in \"\\r\\n\\x00\")\n"
                 "            or not isinstance(reasons, list) or not reasons or len(reasons) > 16\n"
                 "            or any(False or not item\n"),
              lambda m: raises_exact(
                  lambda: m._commit_reasons("1", [1]), ValueError,
                  "reconcile atomic reason fields are invalid")),
        Fence("R6", "commit reasons requires sorted uniqueness",
              mu("            or reasons != sorted(set(reasons))\n", "            or False\n"), p_r6),
        Fence("R7", "commit reasons enforces vocabulary",
              mu("            or any(item not in _VOL_ANOMALY_REASONS for item in reasons)\n",
                 "            or False\n"),
              lambda m: raises_exact(
                  lambda: m._commit_reasons("unknown", ["unknown"]), ValueError,
                  "reconcile atomic reason fields are invalid")),
        Fence("R8", "commit reasons requires joined-field agreement",
              mu('            or reason != ",".join(reasons)):\n',
                 "            or False):\n"),
              lambda m: raises_exact(
                  lambda: m._commit_reasons("jump_up", ["hard_nonfinite"]),
                  ValueError, "reconcile atomic reason fields are invalid")),
        Fence("R9", "commit reasons returns a detached list",
              mu("    return reason, list(reasons)\n\n\ndef _commit_correction",
                 "    return reason, reasons\n\n\ndef _commit_correction"), p_r9),

        Fence("X1", "correction rejects non-dict with exact ValueError",
              mu("    if not isinstance(value, dict):\n"
                 "        raise ValueError(\"reconcile correction evidence fields are invalid\")\n",
                 "    if False:\n"
                 "        raise ValueError(\"reconcile correction evidence fields are invalid\")\n"),
              lambda m: raises_exact(
                  lambda: m._commit_correction(
                      list(correction().items()), normalized=normalized()),
                  ValueError, "reconcile correction evidence fields are invalid")),
        Fence("X2", "correction requires exact fields: missing and extra fail",
              mu("    if set(value) != _CORRECTION_FIELDS:\n"
                 "        raise ValueError(\"reconcile correction evidence fields are invalid\")\n",
                 "    if False:\n"
                 "        raise ValueError(\"reconcile correction evidence fields are invalid\")\n"), p_x2),
        Fence("X3", "correction pins input version",
              mu('    if value.get("version") != 1 \\\n'
                 '            or value.get("type") != "vol_value_refetch_reconcile":\n',
                 '    if False \\\n'
                 '            or value.get("type") != "vol_value_refetch_reconcile":\n'),
              lambda m: raises_exact(
                  lambda: m._commit_correction(
                      correction(version=2), normalized=normalized()), ValueError,
                  "reconcile correction evidence version/type is invalid")),
        Fence("X4", "correction pins input type string",
              mu('    if value.get("version") != 1 \\\n'
                 '            or value.get("type") != "vol_value_refetch_reconcile":\n',
                 '    if value.get("version") != 1 \\\n'
                 '            or False:\n'),
              lambda m: raises_exact(
                  lambda: m._commit_correction(
                      correction(type="other"), normalized=normalized()), ValueError,
                  "reconcile correction evidence version/type is invalid")),
        Fence("X5", "correction requires normalized day agreement",
              mu('    if value.get("day") != normalized["day"]:\n'
                 '        raise ValueError("reconcile correction day disagrees")\n',
                 '    if False:\n'
                 '        raise ValueError("reconcile correction day disagrees")\n'),
              lambda m: raises_exact(
                  lambda: m._commit_correction(
                      correction(day="2026-08-02"), normalized=normalized()),
                  ValueError, "reconcile correction day disagrees")),
        Fence("X6", "correction requires one request",
              mu('    if value.get("request_count") != 1:\n'
                 '        raise ValueError("reconcile correction request count is invalid")\n',
                 '    if False:\n'
                 '        raise ValueError("reconcile correction request count is invalid")\n'),
              lambda m: raises_exact(
                  lambda: m._commit_correction(
                      correction(request_count=2), normalized=normalized()),
                  ValueError, "reconcile correction request count is invalid")),
        Fence("X7", "correction rejects bool bars with strict type",
              mu("    if type(bars) is not int or not 0 < bars <= 2_000_000:\n",
                 "    if not isinstance(bars, int) or not 0 < bars <= 2_000_000:\n"),
              lambda m: raises_exact(
                  lambda: m._commit_correction(
                      correction(bars=True), normalized=normalized()), ValueError,
                  "reconcile correction bar count is invalid")),
        Fence("X8", "correction enforces inclusive bars bounds",
              mu("    if type(bars) is not int or not 0 < bars <= 2_000_000:\n",
                 "    if type(bars) is not int:\n"), p_x8),
        Fence("X9", "correction delegates reason validation",
              mu("    reason, reasons = _commit_reasons(\n"
                 "        value.get(\"reason\"), value.get(\"reasons\"))\n",
                 "    reason, reasons = (\n"
                 "        value.get(\"reason\"), list(value.get(\"reasons\")))\n"),
              lambda m: raises_exact(
                  lambda: m._commit_correction(
                      correction(reason="unknown", reasons=["unknown"]),
                      normalized=normalized(reason="unknown", reasons=["unknown"])),
                  ValueError, "reconcile atomic reason fields are invalid")),
        Fence("X10", "correction checks reason provenance",
              mu('    if (normalized.get("reason") != reason\n'
                 '            or normalized.get("reasons") != reasons):\n',
                 '    if (False\n'
                 '            or normalized.get("reasons") != reasons):\n'),
              lambda m: raises_exact(
                  lambda: m._commit_correction(
                      correction(), normalized=normalized(reason="jump_up")),
                  ValueError, "reconcile correction provenance disagrees")),
        Fence("X11", "correction checks reasons provenance",
              mu('    if (normalized.get("reason") != reason\n'
                 '            or normalized.get("reasons") != reasons):\n',
                 '    if (normalized.get("reason") != reason\n'
                 '            or False):\n'),
              lambda m: raises_exact(
                  lambda: m._commit_correction(
                      correction(), normalized=normalized(
                          reasons=["jump_up"])), ValueError,
                  "reconcile correction provenance disagrees")),
        Fence("X12", "correction enforces what_to_show vocabulary",
              mu("    if what not in _VOL_WHAT_TO_SHOW:\n"
                 "        raise ValueError(\"reconcile correction what_to_show is invalid\")\n",
                 "    if False:\n"
                 "        raise ValueError(\"reconcile correction what_to_show is invalid\")\n"),
              lambda m: raises_exact(
                  lambda: m._commit_correction(
                      correction(what_to_show="TRADES"), normalized=normalized()),
                  ValueError, "reconcile correction what_to_show is invalid")),
        Fence("X13", "correction delegates run text validation",
              mu('        "run": _commit_text(value.get("run"), "correction run"),\n',
                 '        "run": value.get("run"),\n'),
              lambda m: raises_exact(
                  lambda: m._commit_correction(
                      correction(run=""), normalized=normalized()), ValueError,
                  "reconcile correction run is invalid")),
        Fence("X14", "correction delegates source text validation",
              mu('        "source": _commit_text(value.get("source"), "correction source"),\n',
                 '        "source": value.get("source"),\n'),
              lambda m: raises_exact(
                  lambda: m._commit_correction(
                      correction(source=""), normalized=normalized()), ValueError,
                  "reconcile correction source is invalid")),
        Fence("X15", "correction delegates confirmed text validation",
              mu('        "confirmed": _commit_text(\n'
                 '            value.get("confirmed"), "correction confirmed"),\n',
                 '        "confirmed": value.get("confirmed"),\n'),
              lambda m: raises_exact(
                  lambda: m._commit_correction(
                      correction(confirmed=""), normalized=normalized()), ValueError,
                  "reconcile correction confirmed is invalid")),
        Fence("X16", "correction validates old_value when present",
              mu('        "old_value": _commit_number(\n'
                 '            value.get("old_value"), "correction old_value", nullable=True),\n',
                 '        "old_value": value.get("old_value"),\n'),
              lambda m: raises_exact(
                  lambda: m._commit_correction(
                      correction(old_value=float("inf")), normalized=normalized()),
                  ValueError, "reconcile correction old_value must be finite")),
        Fence("X17", "correction permits nullable old_value",
              mu('            value.get("old_value"), "correction old_value", nullable=True),\n',
                 '            value.get("old_value"), "correction old_value"),\n'),
              lambda m: accepts(lambda: m._commit_correction(
                  correction(old_value=None), normalized=normalized()))),
        Fence("X18", "correction requires finite new_value",
              mu('        "new_value": _commit_number(\n'
                 '            value.get("new_value"), "correction new_value"),\n',
                 '        "new_value": value.get("new_value"),\n'), p_x18),
        Fence("X19", "correction validates old-day SHA",
              mu('        "old_day_sha256": _commit_sha(\n'
                 '            value.get("old_day_sha256"), "correction old-day SHA-256"),\n',
                 '        "old_day_sha256": value.get("old_day_sha256"),\n'),
              lambda m: raises_exact(
                  lambda: m._commit_correction(
                      correction(old_day_sha256="x"), normalized=normalized()),
                  ValueError, "reconcile correction old-day SHA-256 is invalid")),
        Fence("X20", "correction validates new-day SHA",
              mu('        "new_day_sha256": _commit_sha(\n'
                 '            value.get("new_day_sha256"), "correction new-day SHA-256"),\n',
                 '        "new_day_sha256": value.get("new_day_sha256"),\n'),
              lambda m: raises_exact(
                  lambda: m._commit_correction(
                      correction(new_day_sha256="x"), normalized=normalized()),
                  ValueError, "reconcile correction new-day SHA-256 is invalid")),
        Fence("X21", "correction validates old-month SHA",
              mu('        "old_month_sha256": _commit_sha(\n'
                 '            value.get("old_month_sha256"), "correction old-month SHA-256"),\n',
                 '        "old_month_sha256": value.get("old_month_sha256"),\n'),
              lambda m: raises_exact(
                  lambda: m._commit_correction(
                      correction(old_month_sha256="x"), normalized=normalized()),
                  ValueError, "reconcile correction old-month SHA-256 is invalid")),
        Fence("X22", "correction validates new-month SHA",
              mu('        "new_month_sha256": _commit_sha(\n'
                 '            value.get("new_month_sha256"), "correction new-month SHA-256"),\n',
                 '        "new_month_sha256": value.get("new_month_sha256"),\n'),
              lambda m: raises_exact(
                  lambda: m._commit_correction(
                      correction(new_month_sha256="x"), normalized=normalized()),
                  ValueError, "reconcile correction new-month SHA-256 is invalid")),
        Fence("X23", "correction emits constant version",
              mu('        "version": 1,\n        "type": "vol_value_refetch_reconcile",\n',
                 '        "version": 2,\n        "type": "vol_value_refetch_reconcile",\n'),
              lambda m: m._commit_correction(
                  correction(), normalized=normalized())["version"] == 1),
        Fence("X24", "correction emits constant type",
              mu('        "version": 1,\n        "type": "vol_value_refetch_reconcile",\n',
                 '        "version": 1,\n        "type": "other",\n'),
              lambda m: m._commit_correction(
                  correction(), normalized=normalized())["type"]
              == "vol_value_refetch_reconcile"),
        Fence("X25", "correction emits constant request_count",
              mu('        "what_to_show": what,\n        "request_count": 1,\n',
                 '        "what_to_show": what,\n        "request_count": 2,\n'),
              lambda m: m._commit_correction(
                  correction(), normalized=normalized())["request_count"] == 1),
        Fence("X26", "correction output preserves exact passthrough shape",
              mu('        "request_count": 1,\n        "bars": bars,\n',
                 '        "request_count": 1,\n        "bar_count": bars,\n'), p_x26),
        Fence("X27", "correction detaches dict subclasses before reads",
              mu("    value = dict(value)\n", "    value = value\n"), p_x27),
    )
    return entries


def redundancy_cases() -> tuple[RedundantClause, ...]:
    return (
        RedundantClause(
            "RR1", "empty reason is also rejected by joined agreement",
            mu("    if (not isinstance(reason, str) or not reason or len(reason) > 320\n",
               "    if (not isinstance(reason, str) or False or len(reason) > 320\n"),
            lambda m: raises_exact(
                lambda: m._commit_reasons("", ["hard_nonfinite"]),
                ValueError, "reconcile atomic reason fields are invalid")),
        RedundantClause(
            "RR2", "reason length is bounded by vocabulary plus join",
            mu("    if (not isinstance(reason, str) or not reason or len(reason) > 320\n",
               "    if (not isinstance(reason, str) or not reason or False\n"),
            lambda m: raises_exact(
                lambda: m._commit_reasons("x" * 321, ["hard_nonfinite"]),
                ValueError, "reconcile atomic reason fields are invalid")),
        RedundantClause(
            "RR3", "reason controls are also rejected by joined agreement",
            mu('            or any(c in reason for c in "\\r\\n\\x00")\n',
               "            or False\n"),
            lambda m: raises_exact(
                lambda: m._commit_reasons(
                    "hard_nonfinite\n", ["hard_nonfinite"]), ValueError,
                "reconcile atomic reason fields are invalid")),
        RedundantClause(
            "RR4", "empty reasons is also rejected by joined agreement",
            mu("            or not isinstance(reasons, list) or not reasons or len(reasons) > 16\n",
               "            or not isinstance(reasons, list) or False or len(reasons) > 16\n"),
            lambda m: raises_exact(
                lambda: m._commit_reasons("hard_nonfinite", []), ValueError,
                "reconcile atomic reason fields are invalid")),
        RedundantClause(
            "RR5", "reason cardinality is bounded by unique vocabulary",
            mu("            or not isinstance(reasons, list) or not reasons or len(reasons) > 16\n",
               "            or not isinstance(reasons, list) or not reasons or False\n"),
            lambda m: raises_exact(
                lambda: m._commit_reasons(
                    ",".join(["hard_nonfinite"] * 17),
                    ["hard_nonfinite"] * 17), ValueError,
                "reconcile atomic reason fields are invalid")),
        RedundantClause(
            "RR6", "empty reason item is also outside vocabulary",
            mu("def _commit_reasons(reason, reasons):\n"
               "    if (not isinstance(reason, str) or not reason or len(reason) > 320\n"
               "            or any(c in reason for c in \"\\r\\n\\x00\")\n"
               "            or not isinstance(reasons, list) or not reasons or len(reasons) > 16\n"
               "            or any(not isinstance(item, str) or not item\n",
               "def _commit_reasons(reason, reasons):\n"
               "    if (not isinstance(reason, str) or not reason or len(reason) > 320\n"
               "            or any(c in reason for c in \"\\r\\n\\x00\")\n"
               "            or not isinstance(reasons, list) or not reasons or len(reasons) > 16\n"
               "            or any(not isinstance(item, str) or False\n"),
            lambda m: raises_exact(
                lambda: m._commit_reasons("", [""]), ValueError,
                "reconcile atomic reason fields are invalid")),
        RedundantClause(
            "RR7", "long reason item is also outside vocabulary",
            mu("                   or len(item) > 320 or any(c in item for c in \"\\r\\n\\x00\")\n",
               "                   or False or any(c in item for c in \"\\r\\n\\x00\")\n"),
            lambda m: raises_exact(
                lambda: m._commit_reasons("x" * 321, ["x" * 321]),
                ValueError, "reconcile atomic reason fields are invalid")),
        RedundantClause(
            "RR8", "controlled reason item is also outside vocabulary",
            mu("                   or len(item) > 320 or any(c in item for c in \"\\r\\n\\x00\")\n",
               "                   or len(item) > 320 or False\n"),
            lambda m: raises_exact(
                lambda: m._commit_reasons("bad\n", ["bad\n"]), ValueError,
                "reconcile atomic reason fields are invalid")),
        RedundantClause(
            "RR9", "what_to_show text checks are subsumed by exact vocabulary",
            mu("    what = _commit_text(\n"
               "        value.get(\"what_to_show\"), \"correction what_to_show\", limit=64)\n",
               "    what = value.get(\"what_to_show\")\n"),
            lambda m: all(raises_exact(
                lambda value=value: m._commit_correction(
                    correction(what_to_show=value), normalized=normalized()),
                ValueError, "reconcile correction what_to_show is invalid")
                for value in (None, 1, "", "x" * 65, "bad\n"))),
    )


def known_defects(module: types.ModuleType) -> tuple[tuple[str, str, bool], ...]:
    overflow = raises_exact(
        lambda: module._commit_number(10 ** 400, "number"), OverflowError)
    version_bool = accepts(lambda: module._commit_correction(
        correction(version=True), normalized=normalized()))
    identity = module._reconcile_identity(
        row(ticker="abc", kind="stock_ibkr"))
    kind_case = identity == ("ABC", "stock_ibkr", "2026-08-03")
    return (
        ("KD1", "huge int escapes as OverflowError", overflow),
        ("KD2", "bool version is accepted as version 1", version_bool),
        ("KD3", "kind remains case-sensitive while ticker uppercases", kind_case),
    )


def run() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    kit = CheckKit()
    kit.section("Row 86 shipped-source custody and inventory")
    before = source_bytes()
    before_hash = digest(before)
    kit.check("SOURCE-CUSTODY pinned fix_data_pipeline.py SHA-256",
              before_hash == EXPECTED_SOURCE_SHA256, before_hash)
    try:
        source = before.decode("utf-8")
        baseline = load_module(source)
        cases = fences()
        redundancies = redundancy_cases()
    except BaseException as exc:
        kit.check("HARNESS-INVENTORY source loads and cases build", False,
                  f"{type(exc).__name__}: {exc}")
        return kit.finish()

    ids = tuple(case.fence_id for case in cases)
    redundant_ids = tuple(case.clause_id for case in redundancies)
    kit.check("HARNESS-INVENTORY final code-derived fence count is 90",
              len(cases) == 90, str(len(cases)))
    kit.check("HARNESS-INVENTORY fence IDs are unique",
              len(set(ids)) == len(ids), repr(ids))
    kit.check("HARNESS-INVENTORY redundancy count is 9 after R2/R4 adjudication",
              len(redundancies) == 9, str(len(redundancies)))
    kit.check("HARNESS-INVENTORY redundancy IDs are unique",
              len(set(redundant_ids)) == len(redundant_ids),
              repr(redundant_ids))

    kit.section("One named baseline check per validator fence")
    for case in cases:
        passed, detail = evaluate(case.probe, baseline)
        kit.check(f"FENCE {case.fence_id} {case.description}", passed, detail)

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
            f"{redundant.description}", proof, detail)

    kit.section("Pinned latent defects; engine intentionally unchanged")
    for defect_id, description, present in known_defects(baseline):
        kit.check(f"KNOWN-DEFECT {defect_id} {description}", present)

    after_hash = digest(source_bytes())
    kit.check("SOURCE-CUSTODY production bytes unchanged during mutations",
              after_hash == before_hash, after_hash)
    return kit.finish()


if __name__ == "__main__":
    raise SystemExit(run())
