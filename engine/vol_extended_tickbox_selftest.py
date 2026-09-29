"""Headless regression tests for Row 46 extended-hours checkbox truthfulness.

The production methods are extracted from ``display_data.py`` with ``ast`` and
compiled into a minimal probe class.  The suite never imports ``display_data``
or tkinter, creates no widgets, and performs no live, port, network, or bank
work.

Run with::

    python -B engine/vol_extended_tickbox_selftest.py
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path


ENGINE_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = ENGINE_ROOT.parent
sys.path.insert(0, str(ENGINE_ROOT))

from check_kit import CheckKit  # noqa: E402


SOURCE = PROJECT_ROOT / "display_data.py"
PROBE_METHODS = {
    "_extended_applicable",
    "_set_extended_checkbox_state",
    "_expand_extended",
    "_storage_find_kind_changed",
    "_storage_ibkr_selection_changed",
}

KIT = CheckKit()
check = KIT.check
section = KIT.section


def _parse_app():
    tree = ast.parse(SOURCE.read_text(encoding="utf-8"), filename=str(SOURCE))
    app = next(
        node for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == "DataViewerApp")
    methods = {
        node.name: node for node in app.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))}
    return app, methods


def _extract_probe(app):
    methods = [
        node for node in app.body
        if isinstance(node, ast.FunctionDef) and node.name in PROBE_METHODS]
    probe = ast.ClassDef(
        name="Probe",
        bases=[],
        keywords=[],
        body=methods,
        decorator_list=[],
    )
    module = ast.Module(body=[probe], type_ignores=[])
    ast.fix_missing_locations(module)
    namespace = {}
    exec(compile(module, "<row46-headless-probe>", "exec"), namespace)
    return namespace["Probe"]


def _call_name(node):
    parts = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if isinstance(node, ast.Name):
        parts.append(node.id)
    return ".".join(reversed(parts))


def _assigned_widget(method, attribute):
    """Whether method stores a Checkbutton backed by the shared BooleanVar."""
    for node in ast.walk(method):
        if not isinstance(node, ast.Assign) or len(node.targets) != 1:
            continue
        target = node.targets[0]
        if not (isinstance(target, ast.Attribute)
                and isinstance(target.value, ast.Name)
                and target.value.id == "self"
                and target.attr == attribute):
            continue
        value = node.value
        if not (isinstance(value, ast.Call)
                and _call_name(value.func) == "ttk.Checkbutton"):
            continue
        keywords = {kw.arg: kw.value for kw in value.keywords}
        variable = keywords.get("variable")
        if (isinstance(variable, ast.Attribute)
                and isinstance(variable.value, ast.Name)
                and variable.value.id == "self"
                and variable.attr == "_storage_tws_extended"):
            return True
    return False


def _has_call(method, name, first_arg=None, second_arg=None):
    for node in ast.walk(method):
        if not isinstance(node, ast.Call) or _call_name(node.func) != name:
            continue
        if first_arg is not None:
            if not node.args or not (isinstance(node.args[0], ast.Constant)
                                     and node.args[0].value == first_arg):
                continue
        if second_arg is not None:
            if len(node.args) < 2 or _call_name(node.args[1]) != second_arg:
                continue
        return True
    return False


class FakeVar:
    def __init__(self, value):
        self.value = value
        self.set_calls = []

    def get(self):
        return self.value

    def set(self, value):
        self.set_calls.append(value)
        self.value = value


class FakeWidget:
    def __init__(self):
        self.options = {}
        self.config_calls = []

    def config(self, **options):
        self.options.update(options)
        self.config_calls.append(dict(options))

    configure = config


class FakeTree:
    def __init__(self):
        self.selected = []
        self.rows = {}

    def selection(self):
        return tuple(self.selected)

    def item(self, item_id):
        return self.rows[item_id]

    def choose(self, tokens):
        self.selected = [f"row-{index}" for index in range(len(tokens))]
        self.rows = {
            f"row-{index}": {"values": ["T", "Name", token]}
            for index, token in enumerate(tokens)}


def _expected_widget(enabled):
    return {
        "state": "normal" if enabled else "disabled",
        "text": "+ extended hours" if enabled
        else "+ extended hours (n/a)",
    }


def _add_stocks_case(Probe, initial):
    probe = Probe()
    shared = FakeVar(initial)
    kind = FakeVar("Trades")
    interval = FakeVar("1m")
    checkbox = FakeWidget()
    probe._storage_tws_extended = shared
    probe._storage_find_kind = kind
    probe._storage_find_iv = interval
    probe._storage_find_iv_cb = FakeWidget()
    probe._storage_find_extended_cb = checkbox
    probe._storage_find_iv_changed = lambda: None

    observed = []
    for value in ("Trades", "Implied Vol", "Hist Vol", "Trades"):
        kind.value = value
        probe._storage_find_kind_changed()
        observed.append(dict(checkbox.options))

    check(
        f"Add Stocks transitions Trades/IV/HVOL/Trades (shared={initial})",
        observed == [
            _expected_widget(True),
            _expected_widget(False),
            _expected_widget(False),
            _expected_widget(True),
        ],
        repr(observed),
    )
    check(
        f"Add Stocks never mutates shared checked value (shared={initial})",
        shared.value is initial and shared.set_calls == [],
        repr((shared.value, shared.set_calls)),
    )


def _update_case(Probe, initial):
    probe = Probe()
    shared = FakeVar(initial)
    checkbox = FakeWidget()
    tree = FakeTree()
    probe._storage_tws_extended = shared
    probe._ibkr_extended_cb = checkbox
    probe._ibkr_tree = tree

    inert = (
        (),
        ("1m-iv",),
        ("1d-hvol",),
        ("1d",),
        ("1m-pre",),
        ("1m-post",),
        ("1m-iv", "1d", "1m-pre"),
    )
    inert_results = []
    for tokens in inert:
        tree.choose(tokens)
        probe._storage_ibkr_selection_changed()
        inert_results.append(dict(checkbox.options))
    check(
        f"Update disables empty/kind/daily/suffixed selections (shared={initial})",
        all(result == _expected_widget(False) for result in inert_results),
        repr(inert_results),
    )

    mixed = (("1m-iv", "1m"), ("1d", "1h"), ("1m-pre", "5s"))
    mixed_results = []
    for tokens in mixed:
        tree.choose(tokens)
        probe._storage_ibkr_selection_changed()
        mixed_results.append(dict(checkbox.options))
    check(
        f"Update keeps mixed Trades selections enabled (shared={initial})",
        all(result == _expected_widget(True) for result in mixed_results),
        repr(mixed_results),
    )
    check(
        f"Update never mutates shared checked value (shared={initial})",
        shared.value is initial and shared.set_calls == [],
        repr((shared.value, shared.set_calls)),
    )


def main():
    print("=== Row 46 extended-hours tickbox selftest (headless) ===\n")
    app, methods = _parse_app()
    missing = sorted(PROBE_METHODS - set(methods))
    check("all production probe methods exist", not missing, repr(missing))
    if missing:
        return KIT.finish()
    Probe = _extract_probe(app)

    section("[A] pure applicability and expansion")
    probe = Probe()
    inert = ((), ("1m-iv",), ("1d-hvol",), ("1d",),
             ("1m-pre",), ("1m-post",), ("1m-iv", "1d"))
    active = (("1m",), ("5s",), ("1h",), ("1d", "1m"),
              ("1m-iv", "2m"))
    check("helper rejects every inert-only token set",
          all(probe._extended_applicable(tokens) is False for tokens in inert))
    check("helper accepts every set containing unsuffixed sub-daily Trades",
          all(probe._extended_applicable(tokens) is True for tokens in active))

    source = [
        ("A", "1d"),
        ("B", "1m"),
        ("B", "1m-pre"),
        ("C", "1d-hvol"),
        ("D", "1h"),
        ("A", "1d"),
        ("E", "5s-post"),
    ]
    expected = [
        ("A", "1d"),
        ("B", "1m"),
        ("B", "1m-pre"),
        ("B", "1m-post"),
        ("C", "1d-hvol"),
        ("D", "1h"),
        ("D", "1h-pre"),
        ("D", "1h-post"),
        ("E", "5s-post"),
    ]
    probe._extended_enabled = lambda: True
    check("extended-on guards daily/kinds and preserves expansion order/dedup",
          probe._expand_extended(source) == expected,
          repr(probe._expand_extended(source)))
    duplicate_source = [("A", "1d"), ("A", "1d"), ("B", "1m")]
    probe._extended_enabled = lambda: False
    off_result = probe._expand_extended(duplicate_source)
    check("extended-off returns an exact independent list, including duplicates",
          off_result == duplicate_source and off_result is not duplicate_source,
          repr(off_result))

    section("[B] Add Stocks widget state")
    for initial in (True, False):
        _add_stocks_case(Probe, initial)

    section("[C] Update widget selection state")
    for initial in (True, False):
        _update_case(Probe, initial)

    section("[D] static construction and event wiring")
    check("Add Stocks stores a shared-variable checkbox handle",
          _assigned_widget(methods["_storage_find_dialog"],
                           "_storage_find_extended_cb"))
    check("Update stores a shared-variable checkbox handle",
          _assigned_widget(methods["_storage_ibkr_open"],
                           "_ibkr_extended_cb"))
    check("Update binds TreeviewSelect to its applicability handler",
          _has_call(
              methods["_storage_ibkr_open"],
              "self._ibkr_tree.bind",
              "<<TreeviewSelect>>",
              "self._storage_ibkr_selection_changed",
          ))
    check("Update refresh initializes checkbox state after rebuilding rows",
          _has_call(methods["_storage_ibkr_refresh"],
                    "self._storage_ibkr_selection_changed"))

    return KIT.finish()


if __name__ == "__main__":
    raise SystemExit(main())
