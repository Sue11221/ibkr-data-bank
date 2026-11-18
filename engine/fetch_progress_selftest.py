"""Offline regression tests for Row 49/55 progress accounting.

Pins the Codex-owned seams that the frozen Claude reference intentionally does
not: stable per-job sequence identity, the resumed series/non-increment guard,
the two GUI drains plus ETA lane parser accepting ``(port P, resumed)``, and
the ticker-named WS8 verification detail remaining visible in the log.
No tkinter import, network, port, or production-bank access.
"""

from __future__ import annotations

import ast
import copy
import queue
import sys
import tempfile
from pathlib import Path

ENGINE_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = ENGINE_ROOT.parent
sys.path.insert(0, str(ENGINE_ROOT))

from check_kit import CheckKit  # noqa: E402
import fetch_progress_reference as reference  # noqa: E402
from testbank import isolated_gates  # noqa: E402


KIT = CheckKit()
check = KIT.check
section = KIT.section

STOCK_SOURCE = (ENGINE_ROOT / "stock_ibkr.py").read_text(encoding="utf-8")
DISPLAY_SOURCE = (PROJECT_ROOT / "display_data.py").read_text(encoding="utf-8")
PROBE_SOURCE = (ENGINE_ROOT / "live_spot_probe.py").read_text(encoding="utf-8")


def _function(tree, name):
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) \
                and node.name == name:
            return node
    raise AssertionError(f"missing function {name}")


def _method(source, name):
    tree = ast.parse(source)
    node = copy.deepcopy(_function(tree, name))
    node.decorator_list = []
    module = ast.Module(body=[node], type_ignores=[])
    ast.fix_missing_locations(module)
    namespace = {}
    exec(compile(module, f"<{name}>", "exec"), namespace)
    return namespace[name]


def _subscript_name(node, owner, key):
    return (
        isinstance(node, ast.Subscript)
        and isinstance(node.value, ast.Name)
        and node.value.id == owner
        and isinstance(node.slice, ast.Constant)
        and node.slice.value == key
    )


def test_static_job_identity():
    section("[A] stable per-job identity and unchanged clean branch")
    tree = ast.parse(STOCK_SOURCE)
    parallel = _function(tree, "gap_fill_parallel")
    job_fn = next(
        node for node in parallel.body
        if isinstance(node, ast.FunctionDef) and node.name == "_job")
    finish_fn = next(
        node for node in parallel.body
        if isinstance(node, ast.FunctionDef) and node.name == "_finish_job")

    increments = [
        node for node in ast.walk(job_fn)
        if isinstance(node, ast.AugAssign)
        and _subscript_name(node.target, "next_job_seq", 0)
        and isinstance(node.op, ast.Add)
        and isinstance(node.value, ast.Constant)
        and node.value.value == 1
    ]
    seq_returns = [
        value
        for node in ast.walk(job_fn)
        if isinstance(node, ast.Dict)
        for key, value in zip(node.keys, node.values)
        if isinstance(key, ast.Constant) and key.value == "seq"
    ]
    id_job = [
        node for node in ast.walk(parallel)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name) and node.func.id == "id"
        and node.args and isinstance(node.args[0], ast.Name)
        and node.args[0].id == "job"
    ]
    direct_requeue = [
        node for node in ast.walk(finish_fn)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "appendleft"
        and len(node.args) == 1
        and isinstance(node.args[0], ast.Name)
        and node.args[0].id == "job"
    ]
    series_keys = [
        node.value for node in ast.walk(parallel)
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == "series_key"
                for target in node.targets)
    ]
    exact_key = any(
        isinstance(value, ast.Tuple) and len(value.elts) == 3
        and _subscript_name(value.elts[0], "job", "seq")
        and isinstance(value.elts[1], ast.Name)
        and value.elts[1].id == "ticker"
        and isinstance(value.elts[2], ast.Name)
        and value.elts[2].id == "interval"
        for value in series_keys
    )
    done_increments = [
        node for node in ast.walk(parallel)
        if isinstance(node, ast.AugAssign)
        and _subscript_name(node.target, "done", 0)
        and isinstance(node.op, ast.Add)
        and isinstance(node.value, ast.Constant)
        and node.value.value == 1
    ]
    resumed_guards = [
        node for node in ast.walk(parallel)
        if isinstance(node, ast.If)
        and isinstance(node.test, ast.UnaryOp)
        and isinstance(node.test.op, ast.Not)
        and isinstance(node.test.operand, ast.Name)
        and node.test.operand.id == "resumed"
    ]
    guarded_increments = [
        child
        for guard in resumed_guards
        for statement in guard.body
        for child in ast.walk(statement)
        if child in done_increments
    ]

    check("jobs receive one explicit monotone integer sequence",
          len(increments) == 1 and len(seq_returns) == 1
          and _subscript_name(seq_returns[0], "next_job_seq", 0))
    check("counter key is exactly (job seq, ticker, interval), never id(job)",
          exact_key and not id_job)
    check("only a first announcement can advance the global numerator",
          len(done_increments) == 1 and guarded_increments == done_increments)
    check("hard reroute preserves the same job object and therefore its seq",
          len(direct_requeue) == 1)
    check("clean and resumed suffixes share the old byte-identical clean line",
          'suffix = f"port {port}, resumed" if resumed else f"port {port}"'
          in STOCK_SOURCE
          and 'say(f"[{n}/{total}] {m.group(1)}  ({suffix})")'
          in STOCK_SOURCE)


def test_exact_reroute():
    section("[B] schedule-independent hard-reroute stream")
    with tempfile.TemporaryDirectory(prefix="fetch_progress_selftest_") as temp:
        report, markers = reference.drive(
            Path(temp), reference.FOUR, [2000, 3000],
            lambda port, attempt: port == 2000 and attempt == 1,
            lambda port: port == 3000)
    plain = [marker for marker in markers if "resumed" not in marker[4]]
    resumed = [marker for marker in markers if "resumed" in marker[4]]
    counts = {}
    for marker in markers:
        key = marker[2:4]
        counts[key] = counts.get(key, 0) + 1
    completed = {(row["ticker"], row["interval"])
                 for row in report.get("series") or []}
    resumed_key = resumed[0][2:4] if len(resumed) == 1 else None
    check("resumed repeats the sole doubled series without advancing numerator",
          len(markers) == 5
          and {marker[1] for marker in markers} == {4}
          and sorted(marker[0] for marker in plain) == [1, 2, 3, 4]
          and len(resumed) == 1
          and resumed[0][4] == "port 3000, resumed"
          and counts.get(resumed_key) == 2
          and all(count == 1 for key, count in counts.items()
                  if key != resumed_key)
          and max(marker[0] for marker in markers) == 4,
          repr(markers))
    check("reroute and report completion remain intact",
          completed == set(reference.FOUR)
          and (report.get("watchdog") or {}).get("rerouted_ports") == [2000])


class _Projector:
    def __init__(self):
        self.calls = []

    def on_marker(self, key, elapsed, port=None):
        self.calls.append((key, elapsed, port))

    def projection(self, _elapsed):
        return 9.0


class _Widget:
    def __init__(self):
        self.calls = []

    def config(self, **kwargs):
        self.calls.append(kwargs)


class _FakeDisplay:
    _BATCH_RATE_WINDOW_S = 120.0
    _BATCH_MIN_SAMPLES = 3

    def __init__(self, label, projector):
        self._batch_live = {
            "lbl": label, "start": 0.0, "i": 0, "n": 4,
            "samples": [], "eta0": None, "eta_at": 0.0,
            "paused_total": 0.0, "paused_at": None,
            "detail": "", "pacing_detail": "", "seeded": True,
        }
        self._batch_proj = projector
        self._batch_tick_on = True
        self._run_start = 0.0

    @staticmethod
    def _fetch_pacing_detail(_raw):
        return None

    @staticmethod
    def _fetch_progress_detail(_raw):
        return None

    @staticmethod
    def _batch_elapsed(_live, _now):
        return 1.0

    @staticmethod
    def _batch_remaining(_live, _elapsed):
        return 9.0

    @staticmethod
    def _batch_with_detail(label, _detail):
        return label

    @staticmethod
    def _batch_visible_detail(detail, pacing_detail):
        return " | ".join(value for value in (detail, pacing_detail) if value)

    @staticmethod
    def _batch_label(i, n, _elapsed, _eta):
        return f"{i}/{n}"


def test_gui_contract():
    section("[C] real GUI drains and ETA lane parser")
    drain_generic = _method(DISPLAY_SOURCE, "_drain_progress_batched")
    drain_ibkr = _method(DISPLAY_SOURCE, "_drain_ibkr_progress")
    bar_update = _method(DISPLAY_SOURCE, "_batch_bar_update")
    resumed = "[1/4] AAA 1m  (port 3000, resumed)"

    fake = _FakeDisplay(_Widget(), _Projector())
    q = queue.Queue()
    q.put(("progress", resumed))
    generic = drain_generic(fake, q)
    check("generic drain retains resumed line as the exact newest marker",
          generic == ([resumed], resumed, None, False, None), repr(generic))

    check("coordinator emitter syntax stays coupled to the WS8 parser",
          'self._say(f"WS8 check: {ticker}...")' in PROBE_SOURCE
          and 'self._say(f"WS8 probe: {ticker}...")' in PROBE_SOURCE)

    ws8_check = "WS8 check: BRK.B..."
    q = queue.Queue()
    q.put(("progress", ws8_check))
    generic = drain_generic(fake, q)
    check("WS8 cross-check names a dotted ticker and keeps its log line",
          generic == (
              [ws8_check], None, None, False,
              {"detail": "Verifying BRK.B — WS8 cross-check",
               "pacing": None}), repr(generic))

    ws8_probe = "WS8 probe: AAPL..."
    q = queue.Queue()
    q.put(("progress", ws8_probe))
    generic = drain_generic(fake, q)
    check("WS8 live probe names its ticker and keeps its log line",
          generic == (
              [ws8_probe], None, None, False,
              {"detail": "Verifying AAPL — WS8 live probe",
               "pacing": None}), repr(generic))

    q = queue.Queue()
    q.put(("progress", ws8_check))
    q.put(("progress", ws8_probe))
    generic = drain_generic(fake, q)
    check("latest WS8 detail wins while both log lines remain",
          generic == (
              [ws8_check, ws8_probe], None, None, False,
              {"detail": "Verifying AAPL — WS8 live probe",
               "pacing": None}), repr(generic))

    near_miss = "WS8 finished: MSFT..."
    q = queue.Queue()
    q.put(("progress", near_miss))
    generic = drain_generic(fake, q)
    check("ordinary WS8-like line does not synthesize a detail",
          generic == ([near_miss], None, None, False, None), repr(generic))

    q = queue.Queue()
    q.put(("progress", resumed))
    ibkr = drain_ibkr(fake, q)
    check("IBKR drain retains resumed line without terminal/event/detail drift",
          ibkr == ([resumed], resumed, None, False, [], None), repr(ibkr))

    label = _Widget()
    bar = _Widget()
    projector = _Projector()
    fake = _FakeDisplay(label, projector)
    plain = "[1/4] AAA 1m  (port 2000)"
    bar_update(fake, plain, bar, label, "_run_start")
    bar_update(fake, resumed, bar, label, "_run_start")
    check("ETA projector keeps exact clean and resumed port lanes",
          projector.calls == [
              ("AAA 1m", 1.0, 2000),
              ("AAA 1m", 1.0, 3000),
          ], repr(projector.calls))
    check("duplicate resumed numerator does not advance or repaint the bar",
          fake._batch_live["i"] == 1
          and bar.calls == [{"maximum": 4, "value": 1}], repr(bar.calls))


def main():
    print("=== Row 49/55 stable progress identity + GUI contract ===\n")
    test_static_job_identity()
    test_exact_reroute()
    test_gui_contract()
    return KIT.finish()


if __name__ == "__main__":
    with isolated_gates():
        raise SystemExit(main())
