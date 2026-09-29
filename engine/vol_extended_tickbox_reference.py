"""Acceptance harness: extended-hours tick box truthfulness for volatility (Row 46).

CLAUDE-OWNED REFERENCE HARNESS - Codex MUST NOT edit; assertion changes require
Claude sign-off on the board (VOL_EXTENDED_TICKBOX_PLAN.md).

Offline and headless: static AST analysis + extracted-function execution against
the display_data.py SOURCE (tkinter is never imported). Reports via the Row 41
check_kit. Exit contract: 1 = check failed; 3 = green but the M1 flag
(display_data.VOL_EXTENDED_TICKBOX = True) is absent; 0 = acceptance.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path
from types import SimpleNamespace

try:                                  # harness hygiene (Row 61): never die on a glyph
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:                     # noqa: BLE001 - older/redirected streams
    pass

ENGINE_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = ENGINE_ROOT.parent
sys.path.insert(0, str(ENGINE_ROOT))

from check_kit import CheckKit  # noqa: E402

SOURCE = PROJECT_ROOT / "display_data.py"

KIT = CheckKit()
check = KIT.check
section = KIT.section


def parse_source():
    return ast.parse(SOURCE.read_text(encoding="utf-8"), filename=str(SOURCE))


def find_method(tree, name):
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if node.name == name:
                return node
    return None


def extract_callable(node):
    """Compile one extracted method into a plain function object."""
    node = ast.parse(ast.unparse(node)).body[0]
    node.decorator_list = []
    module = ast.Module(body=[node], type_ignores=[])
    namespace = {}
    exec(compile(module, f"<extracted {node.name}>", "exec"), namespace)
    return namespace[node.name]


def module_flag(tree, name):
    for node in tree.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == name:
                    if (isinstance(node.value, ast.Constant)
                            and node.value.value is True):
                        return True
    return False


def attribute_names(node):
    return {sub.attr for sub in ast.walk(node)
            if isinstance(sub, ast.Attribute)}


def assigned_self_attributes(node):
    out = set()
    for sub in ast.walk(node):
        if isinstance(sub, ast.Assign):
            for target in sub.targets:
                if isinstance(target, ast.Attribute):
                    out.add(target.attr)
        elif isinstance(sub, ast.Call):
            func = sub.func
            if (isinstance(func, ast.Attribute) and func.attr == "set"
                    and isinstance(func.value, ast.Attribute)):
                out.add(func.value.attr)
    return out


def main():
    print("=== Volatility extended-hours tick box reference (Row 46) ===\n")
    tree = parse_source()
    flag = module_flag(tree, "VOL_EXTENDED_TICKBOX")

    section("[B] baseline: engine refuses kind expansion; UI is static")
    expand_node = find_method(tree, "_expand_extended")
    check("B0 _expand_extended exists in display_data source",
          expand_node is not None)
    expanded = kind_passthrough = daily_expands = None
    if expand_node is not None:
        expand = extract_callable(expand_node)
        on = SimpleNamespace(_extended_enabled=lambda: True)
        expanded = expand(on, [("T", "1m")])
        kind_passthrough = (expand(on, [("T", "1m-iv")]),
                            expand(on, [("T", "1d-hvol")]))
        daily_expands = expand(on, [("T", "1d")])
        check("B1 regular sub-daily expands to exactly {base,-pre,-post} and "
              "kind tokens pass through untouched (the scan answer, pinned)",
              expanded == [("T", "1m"), ("T", "1m-pre"), ("T", "1m-post")]
              and kind_passthrough == ([("T", "1m-iv")], [("T", "1d-hvol")]),
              repr((expanded, kind_passthrough)))
        if not flag:
            check("B2 defect doc: a daily 1d row DOES expand to 1d-pre/-post "
                  "today (sessions are sub-daily; M1 D3 closes this)",
                  daily_expands
                  == [("T", "1d"), ("T", "1d-pre"), ("T", "1d-post")],
                  repr(daily_expands))

    kind_changed = find_method(tree, "_storage_find_kind_changed")
    check("B3a the Add Stocks kind handler exists", kind_changed is not None)
    helper = find_method(tree, "_extended_applicable")
    if not flag:
        refs = attribute_names(kind_changed) if kind_changed else set()
        check("B3b defect doc: no _extended_applicable helper and the kind "
              "handler never touches an extended-checkbox handle today",
              helper is None
              and not any("extended" in name for name in refs),
              repr(sorted(name for name in refs if "extended" in name)))

    if flag:
        section("[F] feature: truthful tick box (M1)")
        check("F1a _extended_applicable helper exists", helper is not None)
        if helper is not None:
            applicable = extract_callable(helper)

            def probe(tokens):
                try:
                    return bool(applicable(SimpleNamespace(), tokens))
                except TypeError:
                    return bool(applicable(tokens))

            check("F1b helper: False for kind/daily-only, True when any "
                  "unsuffixed sub-daily token is present",
                  probe(["1m-iv"]) is False
                  and probe(["1d-hvol"]) is False
                  and probe(["1d"]) is False
                  and probe(["1m-iv", "1d"]) is False
                  and probe(["1m"]) is True
                  and probe(["1m-iv", "1m"]) is True,
                  repr([probe(x) for x in (["1m-iv"], ["1d-hvol"], ["1d"],
                                           ["1m-iv", "1d"], ["1m"],
                                           ["1m-iv", "1m"])]))
        refs = attribute_names(kind_changed) if kind_changed else set()
        check("F2 the kind handler references an extended-checkbox handle",
              any("extended" in name for name in refs), repr(sorted(refs)))
        check("F3 daily-base tokens no longer expand",
              daily_expands == [("T", "1d")], repr(daily_expands))
        mutated = (assigned_self_attributes(kind_changed)
                   if kind_changed else set())
        check("F4 the kind handler never mutates the shared extended variable",
              "_storage_tws_extended" not in mutated, repr(sorted(mutated)))

    if not flag:
        KIT.pending(
            "M1", "display_data.VOL_EXTENDED_TICKBOX absent - not implemented",
            "flag: VOL_EXTENDED_TICKBOX = True (module level)",
            "D1/D2: both '+ extended hours' boxes disable (text notes n/a) "
            "when only volatility/daily is in play; view-state only (D4)",
            "D3: _expand_extended passes daily-base tokens through",
            "D5: pure helper _extended_applicable(tokens) drives both surfaces",
        )
        return KIT.finish(feature_absent=True)
    return KIT.finish()


if __name__ == "__main__":
    raise SystemExit(main())
