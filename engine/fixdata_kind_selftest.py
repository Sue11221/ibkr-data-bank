"""Offline Row 30 CP2 gates for Fix Data kind selection and ratio fills.

The suite uses only fake adapters, temporary storage, and AST-extracted GUI
callbacks.  It never imports ``display_data`` and traps socket construction so
the known port-opening ``stock_ibkr_selftest`` is not needed.
"""

from __future__ import annotations

import ast
import contextlib
from datetime import date, datetime, timezone
import io
from pathlib import Path
import socket
import sys
import tempfile
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parent))

import stock_ibkr as ibkr  # noqa: E402
import stock_storage as storage  # noqa: E402


PASS = [0]
FAIL = [0]
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DISPLAY_SOURCE = PROJECT_ROOT / "display_data.py"
LIVE_CATCHUP_SOURCE = PROJECT_ROOT / "engine" / "live_catchup.py"


def check(condition, label, detail=""):
    if condition:
        PASS[0] += 1
    else:
        FAIL[0] += 1
        suffix = f" ({detail})" if detail else ""
        print(f"  FAIL: {label}{suffix}")


class FakeAdapter:
    def __init__(self, rows, *, use_rth):
        self.rows = list(rows)
        self.use_rth = use_rth
        self.calls = []

    def fetch(self, _contract, end_dt, duration, bar_size,
              what_to_show="TRADES"):
        self.calls.append((self.use_rth, end_dt, duration, bar_size,
                           what_to_show))
        return list(self.rows)


def raw(day_or_dt, o, h, lo, c, volume):
    return SimpleNamespace(date=day_or_dt, open=o, high=h, low=lo,
                           close=c, volume=volume)


def result_shell():
    return {"blocked_months": [], "dup_existing": 0, "conflicts": 0,
            "months": {}, "added": 0, "written": 0}


def fetch_cases():
    daily_iv = FakeAdapter([
        raw(date(2024, 5, 31), 0.10, 0.12, 0.09, 0.11, -1.0),
        raw(date(2024, 6, 17), 0.20, 0.24, 0.18, 0.22, -1.0),
    ], use_rth=False)
    iv_bars, iv_skipped = ibkr._fetch_month_bars(
        daily_iv, object(), 2024, 6, "1d-iv")
    expected_iv = [(datetime(2024, 6, 17),
                    0.20, 0.24, 0.18, 0.22, 0)]
    check(iv_bars == expected_iv and iv_skipped == set(),
          "1d-IV keeps ratio OHLC/-1 sentinel and excludes adjacent month")
    check(len(daily_iv.calls) == 1
          and daily_iv.calls[0][0] is True
          and daily_iv.calls[0][1] == datetime(2024, 6, 30, 23, 59)
          and daily_iv.calls[0][2:] == (
              "2 M", "1 day", "OPTION_IMPLIED_VOLATILITY")
          and daily_iv.use_rth is False,
          "1d-IV pins RTH for the request and restores borrowed state")

    daily_hvol = FakeAdapter([
        raw(date(2024, 6, 17), 0.0, 0.0, 0.0, 0.0, -1.0),
        raw(date(2024, 6, 18), 0.25, 0.30, 0.20, 0.27, 12.75),
    ], use_rth=False)
    hvol_bars, hvol_skipped = ibkr._fetch_month_bars(
        daily_hvol, object(), 2024, 6, "1d-hvol")
    expected_hvol = [
        (datetime(2024, 6, 17), 0.0, 0.0, 0.0, 0.0, 0),
        (datetime(2024, 6, 18), 0.25, 0.30, 0.20, 0.27, 0),
    ]
    check(hvol_bars == expected_hvol and hvol_skipped == set(),
          "1d-HVOL keeps flat zero and fractional-sentinel bars")
    check(len(daily_hvol.calls) == 1
          and daily_hvol.calls[0][0] is True
          and daily_hvol.calls[0][2:] == (
              "2 M", "1 day", "HISTORICAL_VOLATILITY")
          and daily_hvol.use_rth is False,
          "1d-HVOL uses the kind request and restores session state")

    minute_iv = FakeAdapter([
        raw(datetime(2024, 6, 14, 13, 29, tzinfo=timezone.utc),
            0.18, 0.19, 0.17, 0.18, -1.0),       # 09:29 ET: exclude
        raw(datetime(2024, 6, 14, 13, 30, tzinfo=timezone.utc),
            0.20, 0.21, 0.19, 0.20, -1.0),       # 09:30 ET: keep
        raw(datetime(2024, 6, 14, 19, 59, tzinfo=timezone.utc),
            0.25, 0.27, 0.24, 0.26, 12.75),      # 15:59 ET: keep
        raw(datetime(2024, 6, 14, 20, 0, tzinfo=timezone.utc),
            0.26, 0.28, 0.25, 0.27, 12.75),      # 16:00 ET: exclude
    ], use_rth=True)
    minute_bars, minute_skipped = ibkr._fetch_month_bars(
        minute_iv, object(), 2024, 6, "1m-iv")
    expected_minute = [
        (datetime(2024, 6, 14, 9, 30), 0.20, 0.21, 0.19, 0.20, 0),
        (datetime(2024, 6, 14, 15, 59), 0.25, 0.27, 0.24, 0.26, 0),
    ]
    expected_ends = [datetime(2024, 6, d, 20) for d in (30, 23, 16, 9, 2)]
    check(minute_bars == expected_minute and minute_skipped == set(),
          "1m-IV filters mixed payload to both RTH boundaries")
    check([call[1] for call in minute_iv.calls] == expected_ends
          and all(call[0] is True
                  and call[2:] == (
                      "1 W", "1 min", "OPTION_IMPLIED_VOLATILITY")
                  for call in minute_iv.calls)
          and minute_iv.use_rth is True,
          "1m-IV weekly walk uses its canonical RTH envelope then restores state")

    class RaisingAdapter:
        def __init__(self):
            self.use_rth = False

        def fetch(self, *_args, **_kwargs):
            raise RuntimeError("fixture failure")

    raising = RaisingAdapter()
    try:
        ibkr._fetch_month_bars(raising, object(), 2024, 6, "1d-iv")
    except RuntimeError:
        restored = raising.use_rth is False
    else:
        restored = False
    check(restored, "daily fetch restores adapter session state on exception")

    return {"1d-iv": expected_iv,
            "1d-hvol": expected_hvol,
            "1m-iv": expected_minute}


def commit_round_trip(cases):
    with tempfile.TemporaryDirectory(prefix="fixdata-kind-") as tmp:
        root = Path(tmp) / "Stock Data Storage"
        root.mkdir()
        ticker = "RATIO"
        for interval, bars in cases.items():
            res = result_shell()
            conflicts = []
            ibkr._commit_month(
                root, ticker, interval, (2024, 6), bars,
                "row30-test", "fake", res, conflicts.append)
            path = storage.find_month_file(root, ticker, 2024, 6, interval)
            stored, stats = storage.read_month_file(path)
            check(stored == bars and stats["rows"] == len(bars)
                  and res["added"] == len(bars) and res["written"] == 1
                  and not res["blocked_months"] and not conflicts,
                  f"{interval} fetch output survives strict commit/read")
            before = path.read_bytes()
            duplicate = result_shell()
            ibkr._commit_month(
                root, ticker, interval, (2024, 6), bars,
                "row30-test", "fake", duplicate, conflicts.append)
            check(path.read_bytes() == before
                  and duplicate["added"] == 0
                  and duplicate["written"] == 0
                  and duplicate["dup_existing"] == len(bars),
                  f"{interval} identical recommit is byte-stable duplicates")

        manifest = storage.load_manifest(root / ticker)
        check(set(manifest["intervals"]) == set(cases),
              "manifest keeps distinct IV/HVOL interval sections")
        check(storage.find_month_file(root, ticker, 2024, 6, "1d") is None
              and storage.find_month_file(
                  root, ticker, 2024, 6, "1m") is None,
              "ratio commits never create bare TRADES files")


def call_name(node):
    parts = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if isinstance(node, ast.Name):
        parts.append(node.id)
    return ".".join(reversed(parts))


def class_parts(tree):
    app = next(node for node in tree.body
               if isinstance(node, ast.ClassDef)
               and node.name == "DataViewerApp")
    methods = {node.name: node for node in app.body
               if isinstance(node, ast.FunctionDef)}
    assigns = {}
    for node in app.body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1 \
                and isinstance(node.targets[0], ast.Name):
            assigns[node.targets[0].id] = node.value
    return app, methods, assigns


def execute_function(node, globals_dict=None):
    module = ast.Module(body=[node], type_ignores=[])
    ast.fix_missing_locations(module)
    ns = dict(globals_dict or {})
    exec(compile(module, str(DISPLAY_SOURCE), "exec"), ns)  # noqa: S102
    return ns[node.name]


def gui_contract():
    source = DISPLAY_SOURCE.read_text(encoding="utf-8")
    tree = ast.parse(source)
    _app, methods, assigns = class_parts(tree)
    stages = ast.literal_eval(assigns["_FIXDATA_STAGES"])
    options = ast.literal_eval(assigns["_FIXDATA_KIND_OPTIONS"])
    check(stages == ((1, "①  Scan for gaps"),
                     (2, "②  Refetch missing days"),
                     (3, "③  Cross-check accuracy")),
          "staged Fix Data labels exist after Row 27 extraction")
    check(options == (("", "Price (TRADES)"),
                      ("iv", "Implied volatility (IV)"),
                      ("hvol", "Historical volatility (HVOL)")),
          "selector exposes exactly Price, IV, and HVOL")

    selected_fn = execute_function(methods["_fixdata_selected_kinds"])

    class Var:
        def __init__(self, value):
            self.value = value

        def get(self):
            return self.value

    observed = {}
    for mask in range(8):
        fake = SimpleNamespace(
            _FIXDATA_KIND_OPTIONS=options,
            _fixdata_kind_vars={
                kind: Var(bool(mask & (1 << index)))
                for index, (kind, _label) in enumerate(options)})
        observed[mask] = selected_fn(fake)
    expected = {
        mask: tuple(kind for index, (kind, _label) in enumerate(options)
                    if mask & (1 << index))
        for mask in range(8)
    }
    check(observed == expected,
          "all seven non-empty selector combinations and empty map in order")

    opened = methods["_storage_fixdata_open"]
    defaults = [call for call in ast.walk(opened) if isinstance(call, ast.Call)
                and call_name(call.func) == "tk.BooleanVar"
                and any(keyword.arg == "value"
                        and isinstance(keyword.value, ast.Constant)
                        and keyword.value.value is True
                        for keyword in call.keywords)]
    check(len(defaults) == 1,
          "selector construction defaults every kind option on")

    start = methods["_fixdata_start"]
    calls = [node for node in ast.walk(start) if isinstance(node, ast.Call)]
    selected_call = next(node for node in calls
                         if call_name(node.func)
                         == "self._fixdata_selected_kinds")
    port_call = next(node for node in calls
                     if call_name(node.func) == "self._tws_ports")
    pipeline_call = next(node for node in calls
                         if call_name(node.func) == "fix_data_pipeline.run")
    keywords = {keyword.arg: keyword.value for keyword in pipeline_call.keywords}
    empty_returns = [node for node in start.body if isinstance(node, ast.If)
                     and isinstance(node.test, ast.UnaryOp)
                     and isinstance(node.test.op, ast.Not)
                     and isinstance(node.test.operand, ast.Name)
                     and node.test.operand.id == "kinds"
                     and any(isinstance(item, ast.Return) for item in node.body)]
    check(selected_call.lineno < port_call.lineno
          and empty_returns
          and isinstance(keywords.get("kinds"), ast.Name)
          and keywords["kinds"].id == "kinds",
          "selection freezes and rejects empty before TWS/worker dispatch")

    engine_run = next(node for node in start.body
                      if isinstance(node, ast.FunctionDef)
                      and node.name == "_engine_run")
    worker_gets = [node for node in ast.walk(engine_run)
                   if isinstance(node, ast.Call)
                   and isinstance(node.func, ast.Attribute)
                   and node.func.attr == "get"
                   and "_fixdata_kind_vars" in ast.unparse(node.func.value)]
    check(not worker_gets,
          "worker never reads Tk selector variables")

    series_fn_node = next(node for node in ast.walk(engine_run)
                          if isinstance(node, ast.FunctionDef)
                          and node.name == "_series")
    fake_self = SimpleNamespace(_storage_known_series=lambda: [
        ("AAA", {"1m", "5m", "1d", "1m-pre", "1m-post",
                 "1m-iv", "1d-hvol"})])
    series_fn = execute_function(
        series_fn_node, {"self": fake_self, "stock_storage": storage})
    check(set(series_fn("bank")) == {("AAA", "1m"), ("AAA", "5m")},
          "stage ③ is explicit intraday TRADES/RTH only")

    internal_check = methods["_internal_check_after_fetch"]
    internal_names = {call_name(node.func) for node in ast.walk(internal_check)
                      if isinstance(node, ast.Call)}
    internal_hyphens = [node for node in ast.walk(internal_check)
                        if isinstance(node, ast.Constant)
                        and node.value == "-"]
    check("stock_storage.kind_of" in internal_names
          and "stock_storage.session_of" in internal_names
          and not internal_hyphens,
          "post-fetch accuracy check is explicit TRADES/RTH only")

    heal = methods["_storage_heal_gaps"]
    heal_work = next(node for node in ast.walk(heal)
                     if isinstance(node, ast.FunctionDef)
                     and node.name == "_work")
    queued = []
    adapter_calls = []

    class FakeLiveIB:
        def __init__(self, *_args, **_kwargs):
            adapter_calls.append("construct")

        def connect(self):
            adapter_calls.append("connect")
            return self

    heal_fn = execute_function(heal_work, {
        "series": None,
        "root": "bank",
        "ports": [7497],
        "q": SimpleNamespace(put=queued.append),
        "self": SimpleNamespace(_seal_aborted=lambda: False),
        "stock_storage": storage,
        "stock_validate": SimpleNamespace(
            scan_all_gaps=lambda *_a, **_k: {
                "error": "prior report cannot be preserved",
                "summary": {"AAA 1m": {
                    "missing_day_list": ["2024-06-17"]}},
            }),
        "stock_ibkr": SimpleNamespace(
            LiveIB=FakeLiveIB, HOST_DEFAULT="127.0.0.1",
            CLIENT_ID_FETCH=1),
    })
    heal_fn()
    check(queued and "cannot be preserved" in (queued[0]["error"] or "")
          and not adapter_calls,
          "manual gap heal stops before adapter creation when preservation fails")

    d7_methods = ("_storage_ibkr_finalize", "_gap_heal_after_fetch",
                  "_storage_heal_gaps", "_gap_scan_after_fetch",
                  "_storage_find_finalize")
    d7_ok = True
    for name in d7_methods:
        body = methods[name]
        names = {call_name(node.func) for node in ast.walk(body)
                 if isinstance(node, ast.Call)}
        stale = [node for node in ast.walk(body)
                 if isinstance(node, ast.Constant) and node.value == "-"]
        d7_ok = d7_ok and "stock_storage.session_of" in names and not stale
    check(d7_ok, "all five post-fetch gap surfaces include RTH kind tokens")

    label_nodes = [methods[name] for name in (
        "_storage_kind_key", "_storage_kind_name", "_storage_data_label")]
    probe_class = ast.ClassDef(
        name="Probe", bases=[], keywords=[], body=label_nodes,
        decorator_list=[])
    module = ast.Module(body=[probe_class], type_ignores=[])
    ast.fix_missing_locations(module)
    ns = {"stock_storage": storage}
    exec(compile(module, str(DISPLAY_SOURCE), "exec"), ns)  # noqa: S102
    Probe = ns["Probe"]
    check(Probe._storage_data_label("1d-iv") == "IV"
          and Probe._storage_data_label("1d-hvol") == "HVOL",
          "Gaps table labels ratio kinds readably")
    gap_source = ast.get_source_segment(source, methods["_storage_gap_show"])
    check('key = f"{str(ticker).strip().upper()} {str(interval).strip()}"'
          in gap_source and "GAP MAP — {ticker} {interval}" in gap_source,
          "Gap Map retains exact AAPL 1d-iv-style keys and heading")


def catchup_error_contract():
    source = LIVE_CATCHUP_SOURCE.read_text(encoding="utf-8")
    tree = ast.parse(source)
    main_node = next(node for node in tree.body
                     if isinstance(node, ast.FunctionDef)
                     and node.name == "main")
    fake_sv = SimpleNamespace(
        load_cross_validation=lambda _root: {},
        discover_series=lambda *_a, **_k: [],
        scan_all_gaps=lambda *_a, **_k: {
            "error": "prior report cannot be preserved",
            "summary": {},
        },
        FULL_HISTORY_RANGE="full",
    )
    main_fn = execute_function(main_node, {
        "_bank_root": lambda: "bank",
        "sv": fake_sv,
        "datetime": datetime,
        "sys": sys,
    })
    stdout = io.StringIO()
    stderr = io.StringIO()
    with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
        rc = main_fn()
    check(rc == 1 and "gap scan failed" in stderr.getvalue()
          and "DONE." not in stdout.getvalue(),
          "offline catch-up reports preservation failure instead of success")


def main():
    original_socket = socket.socket

    def forbidden_socket(*_args, **_kwargs):
        raise AssertionError("network/socket use is forbidden in this suite")

    socket.socket = forbidden_socket
    try:
        cases = fetch_cases()
        commit_round_trip(cases)
        gui_contract()
        catchup_error_contract()
    finally:
        socket.socket = original_socket
    total = PASS[0] + FAIL[0]
    print(f"fixdata kind: {PASS[0]}/{total} passed, {FAIL[0]} failed")
    return 1 if FAIL[0] else 0


if __name__ == "__main__":
    raise SystemExit(main())
