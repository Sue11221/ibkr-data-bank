"""Fail-closed custody of explicit operation roots and their GUI dispatches."""
import argparse
import ast
import json
import os
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
ARTIFACT = Path(__file__).with_suffix(".json")
APIS = frozenset({"begin_operation", "_OperationHandle", "_ChildHandle"})
TEST_OWNER = "engine/fetch_a2_ibkr_workflows_selftest.py"
CONTEXT_TEST_OWNERS = frozenset({
    TEST_OWNER,
    "engine/fetch_a2_infrastructure_selftest.py",
    "engine/fetch_a2_http_workflows_selftest.py",
    "engine/fetch_horizon_selftest.py",
    "engine/fetch_ibkr_integration_selftest.py",
    "engine/update_data_fence_reference.py",
})
CONTEXT_NAME = "FetchRunContext"
STOCK_IBKR_ROOT_PURPOSES = {
    "nightly_gap_fill": "nightly",
    "nightly_gap_fill_parallel": "nightly",
    "nightly_gap_fill_parallel_resilient": "nightly",
    "nightly_update": "nightly",
    "add_stocks_gap_fill": "add_stocks",
    "add_stocks_gap_fill_parallel_resilient": "add_stocks",
    "find_symbol": "identity",
    "company_lookup": "identity",
    "validate_symbols": "identity",
    "doctor": "diagnostic",
    "estimate_backfill": "diagnostic",
    "probe_max_span": "diagnostic",
    "fill_missing_days": "repair",
}
ROOT_PURPOSES = {
    **STOCK_IBKR_ROOT_PURPOSES,
    "validate_series": "validation",
    "cross_validate_ticker": "validation",
    "revalidate_library": "validation",
    "fetch_daily_reference_single_request": "validation",
    "sweep_bank": "external_sweep",
    "sweep_ticker": "external_sweep",
}
# Mint ownership is module + qualified definition, never just a spelling.
ROOT_OWNERS = {("engine/stock_ibkr.py", name): purpose
               for name, purpose in STOCK_IBKR_ROOT_PURPOSES.items()}
ROOT_OWNERS[("engine/live_spot_probe.py", "run_live_probe")] = "diagnostic"
ROOT_OWNERS[("engine/fix_data_pipeline.py", "run")] = "repair"
ROOT_OWNERS[("engine/live_internal_revalidate.py", "run_internal_revalidate")] = "diagnostic"
ROOT_OWNERS[("engine/live_pipeline_check.py", "run_pipeline_check")] = "diagnostic"
ROOT_OWNERS[("engine/live_internal_daily_check.py", "run_internal_daily_check")] = "diagnostic"
ROOT_OWNERS[("engine/live_feature_check.py", "run_feature_check")] = "diagnostic"
ROOT_OWNERS[("engine/live_combined_flags.py", "run_combined_flags")] = "diagnostic"
ROOT_OWNERS[("engine/live_kind_smoke.py", "run_kind_smoke")] = "diagnostic"
ROOT_OWNERS[("engine/two_account_pacing_probe.py", "run_pacing_probe")] = "diagnostic"
ROOT_OWNERS[("engine/live_run_all.py", "run_all")] = "diagnostic"
ROOT_OWNERS[("engine/live_validate_catchup.py", "run_catchup")] = "diagnostic"
for name in ("validate_series", "cross_validate_ticker",
             "revalidate_library", "fetch_daily_reference_single_request"):
    ROOT_OWNERS[("engine/stock_validate.py", name)] = "validation"
for name in ("sweep_bank", "sweep_ticker"):
    ROOT_OWNERS[("engine/external_sweep.py", name)] = "external_sweep"
TRANSITIVE_APIS = {"preflight": "diagnostic"}
DISPATCH_PURPOSES = {**ROOT_PURPOSES, **TRANSITIVE_APIS, "run_live_probe": "diagnostic",
                     "run_internal_revalidate": "diagnostic", "run_pipeline_check": "diagnostic",
                     "run_internal_daily_check": "diagnostic", "run_feature_check": "diagnostic",
                     "run_combined_flags": "diagnostic", "run_kind_smoke": "diagnostic"}
DISPATCH_PURPOSES["run_pacing_probe"] = "diagnostic"
DISPATCH_PURPOSES["run_all"] = "diagnostic"
DISPATCH_PURPOSES["run_catchup"] = "diagnostic"
ROOT_API_NAMES = set(ROOT_PURPOSES) | {"run_live_probe", "run_internal_revalidate",
                                      "run_pipeline_check", "run_internal_daily_check",
                                      "run_feature_check", "run_combined_flags",
                                      "run_kind_smoke"}
ROOT_API_NAMES.add("run_pacing_probe")
ROOT_API_NAMES.add("run_all")
ROOT_API_NAMES.add("run_catchup")
GUI_DISPATCHES = [
    ("DataViewerApp._find_search_loop", "find_symbol"),
    ("DataViewerApp._storage_find_build._work", "validate_symbols"),
    ("DataViewerApp._storage_find_fetch._work", "estimate_backfill"),
    ("DataViewerApp._storage_find_start_fill._work", "add_stocks_gap_fill"),
    ("DataViewerApp._storage_find_start_fill._work", "add_stocks_gap_fill_parallel_resilient"),
    ("DataViewerApp._storage_ibkr_start._work", "nightly_update"),
    ("DataViewerApp._storage_ibkr_start._work", "nightly_update"),
    ("DataViewerApp._finalize_drain_and_seal", "fill_missing_days"),
    ("DataViewerApp._storage_heal_gaps._work", "fill_missing_days"),
    ("DataViewerApp._storage_find_build._work", "preflight"),
]
GUI_HTTP_DISPATCHES = [
    ("DataViewerApp._storage_xval_revalidate._work_body", "cross_validate_ticker", "stock_validate"),
    ("DataViewerApp._xval_worker", "cross_validate_ticker", "stock_validate"),
    ("DataViewerApp._storage_verify_dialog.run.work", "validate_series", "stock_validate"),
    ("DataViewerApp._addstock_consume_portfree_debt._work", "cross_validate_ticker", "stock_validate"),
    ("DataViewerApp._storage_find_start_fill._probe_check", "cross_validate_ticker", "stock_validate"),
]
ENGINE_DISPATCHES = [
    ("engine/live_combined_flags.py", "main", "run_combined_flags", None),
    ("engine/live_kind_smoke.py", "main", "run_kind_smoke", None),
    ("engine/two_account_pacing_probe.py", "main", "run_pacing_probe", None),
    ("engine/live_run_all.py", "main", "run_all", None),
    ("engine/live_validate_catchup.py", "main", "run_catchup", None),
    ("engine/live_feature_check.py", "main", "run_feature_check", None),
    ("engine/live_internal_revalidate.py", "main", "run_internal_revalidate", None),
    ("engine/live_pipeline_check.py", "main", "run_pipeline_check", None),
    ("engine/live_internal_daily_check.py", "main", "run_internal_daily_check", None),
    ("engine/live_spot_probe_cli.py", "main", "run_live_probe", "probe"),
    ("engine/live_feature_check.py", "_feature_body", "validate_symbols", "sk"),
    ("engine/live_internal_daily_check.py", "_daily_body", "validate_symbols", "sk"),
    ("engine/live_kind_smoke.py", "test_end_to_end_store", "validate_symbols", "sk"),
    ("engine/live_pipeline_check.py", "_pipeline_body", "validate_symbols", "sk"),
    ("engine/stock_ibkr.py", "_nightly_update_body", "validate_symbols", None),
    ("engine/stock_ibkr.py", "preflight", "doctor", None),
    ("engine/stock_ibkr.py", "_backfill_seal_interior", "fill_missing_days", None),
    ("engine/fix_data_pipeline.py", "_fixed_fill", "fill_missing_days", "sk"),
    ("display_data.py", "DataViewerApp._fixdata_start._engine_run", "run", "fix_data_pipeline"),
    ("engine/live_catchup.py", "main", "cross_validate_ticker", "sv"),
    ("engine/external_sweep_cli.py", "main", "sweep_bank", "sweep"),
]
CHILD_DISPATCHES = {
    ("engine/live_kind_smoke.py", "test_end_to_end_store", "validate_symbols"),
    ("engine/live_feature_check.py", "_feature_body", "validate_symbols"),
    ("engine/live_pipeline_check.py", "_pipeline_body", "validate_symbols"),
    ("engine/live_internal_daily_check.py", "_daily_body", "validate_symbols"),
    ("engine/stock_ibkr.py", "_nightly_update_body", "validate_symbols"),
    ("engine/stock_ibkr.py", "_backfill_seal_interior", "fill_missing_days"),
    ("engine/fix_data_pipeline.py", "_fixed_fill", "fill_missing_days"),
}
FORWARDED_REFERENCES = set()


def _dotted(node):
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        prefix = _dotted(node.value)
        return prefix + "." + node.attr if prefix else None
    return None


def _path_metadata(node, parents, path):
    """Only the two inert reviewed A1 call-path tuple entries are literals."""
    if path != "engine/fetch_send_inventory.py" or not isinstance(node, ast.Constant):
        return None
    sequence = parents.get(node)
    mapping = parents.get(sequence)
    assignment = parents.get(mapping)
    if (not isinstance(sequence, ast.Tuple) or not isinstance(mapping, ast.Dict)
            or not isinstance(assignment, ast.AnnAssign)
            or not isinstance(assignment.target, ast.Name)
            or assignment.target.id != "A1_CALL_PATHS" or assignment.value is not mapping):
        return None
    index = next((i for i, value in enumerate(mapping.values) if value is sequence), None)
    key = mapping.keys[index] if index is not None else None
    if (isinstance(key, ast.Constant) and key.value in
            {"ibkr.gap_fill.month_daily", "ibkr.gap_fill.month_intraday"}
            and node.value == "fill_missing_days"):
        return key.value
    return None


def _reference(node, names):
    # These AST categories are disjoint. Once classified, a nonmatching name
    # cannot become a reference by testing the other categories again.
    if isinstance(node, ast.Name):
        return node.id in names
    if isinstance(node, ast.Attribute):
        return node.attr in names
    if isinstance(node, ast.alias):
        return node.name in names
    return (isinstance(node, ast.Constant) and isinstance(node.value, str)
            and node.value in names)


def _test_source(path):
    # A test-looking suffix is not a classification. Only the shipped runner
    # manifests and these two pre-existing standalone queue drivers are exempt
    # from the production dispatch denominator (never from mint confinement).
    import run_gates
    names = (set(run_gates.CORPUS_SELFTEST_NAMES) | set(run_gates.REFERENCE_SUITE_NAMES)
             | {"adaptive_concurrency_test", "dynamic_queue_test"})
    return Path(path).parent.as_posix() == "engine" and Path(path).stem in names


def source_catalog():
    # Cover source in arbitrary project folders without launching Git (offline
    # test tripwires forbid subprocesses, and an exported copy has no .git).
    # Runtime banks/evidence and historical code snapshots are not shipped
    # source owners. No filename suffix grants a construction exemption.
    excluded = {".git", "Stock Data Storage", "Data storage", "Run Logs",
        "_derived_daily_cache", "Validation Reference", "_code_snapshots",
        "archive/_code_snapshots", "_quarantine", "_post_ingest_verify"}
    paths = []
    for directory, dirs, files in os.walk(ROOT, followlinks=False):
        parent = Path(directory)
        kept = []
        for name in dirs:
            path = parent / name
            if name == "__pycache__" or path.relative_to(ROOT).as_posix() in excluded:
                continue
            if path.is_symlink() or path.is_junction():
                raise ValueError("source catalog refuses linked directory: " + str(path))
            kept.append(name)
        dirs[:] = kept
        for name in files:
            if name.endswith(".py"):
                path = parent / name
                if path.is_symlink():
                    raise ValueError("source catalog refuses linked source: " + str(path))
                paths.append(path)
    return {p.relative_to(ROOT).as_posix(): p.read_text(encoding="utf-8-sig") for p in paths}


def _source_index(source):
    """One fresh parse and child walk; never cache across source checks."""
    tree = ast.parse(source)
    nodes, parents, children, references = [tree], {}, {}, {}
    # Breadth-first order and last-parent handling match ast.walk, including
    # AST's shared operator/context singleton nodes.
    for node in nodes:
        # Match iter_child_nodes without allocating its nested field/child
        # generators for every node. Absent optional fields have no edge;
        # AST-valued list entries retain their original field/list order.
        edges = []
        for field in node._fields:
            value = getattr(node, field, None)
            if isinstance(value, list):
                edges.extend(item for item in value if isinstance(item, ast.AST))
            elif isinstance(value, ast.AST):
                edges.append(value)
        edges = tuple(edges)
        children[node] = edges
        # Classify once for the context, mint, root and transitive scans. Keep
        # string literals as well as executable names: indirect references
        # must remain visible. This index is owned by this fresh parse only.
        if isinstance(node, ast.Name):
            references[node] = node.id
        elif isinstance(node, ast.Attribute):
            references[node] = node.attr
        elif isinstance(node, ast.alias):
            references[node] = node.name
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            references[node] = node.value
        for child in edges:
            parents[child] = node
            nodes.append(child)
    return tree, nodes, parents, children, references


def _indexed_sources(sources, retained=None):
    # Parse lazily in the existing sorted order: an earlier context refusal
    # must still precede a later file's syntax error. Retention is per build.
    for path, source in sorted(sources.items()):
        indexed = _source_index(source)
        yield path, indexed
        # Context custody receives the complete index before any compaction.
        # Registered tests other than the mint owner have exactly one later
        # obligation: reject every mint reference. Preserve that fact, not a
        # second retained copy of all their syntax. Refusal is still deferred
        # until after context validation of the entire catalog.
        if retained is not None:
            if ("/" in path and path.split("/", 1)[0] not in {"engine", "tools", "ops"}
                    or path in {"engine/fetch_operations.py", "engine/fetch_operation_inventory.py"}):
                continue  # These paths had no later root/dispatch analysis.
            if (_test_source(path) and path != TEST_OWNER
                    and path not in {module for module, owner in ROOT_OWNERS}):
                retained[path] = not APIS.isdisjoint(indexed[4].values())
            else:
                retained[path] = indexed


def _context_inventory(sources):
    return _context_inventory_indexes(_indexed_sources(sources))


def _source_order(node, parents, children):
    """Recover preorder for sparse references, preserving first-refusal order."""
    positions = []
    while node in parents:
        parent = parents[node]
        positions.append(children[parent].index(node))
        node = parent
    return tuple(reversed(positions))


def _qualified_owner(node, parents):
    names = []
    while node is not None:
        if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            names.append(node.name)
        node = parents.get(node)
    return ".".join(reversed(names))


def _context_inventory_indexes(indexes):
    """Inventory references, not just calls: aliases cannot hide a constructor.

    The only production mint is the core's direct .create call. The context
    implementation's type annotations/isinstance check and stock_ibkr's legacy
    import are read-only exceptions, not file-wide construction exemptions.
    Offline owners are an explicit list AND must remain runner-registered.
    Dynamic code containing the class name is a reference too. This is a
    fail-closed source fence, not a sandbox for arbitrary adversarial Python.
    """
    references, constructors = [], []
    for path, (tree, _nodes, parents, children, reference_names) in indexes:
        offline = path in CONTEXT_TEST_OWNERS
        if offline and not _test_source(path):
            raise ValueError("context test owner is no longer registered: " + path)

        # Only names/attributes/aliases/string constants can be references.
        # Filter their already-fresh index, then retain the former recursive
        # preorder (including nested-definition owners and first refusal).
        candidates = [node for node, spelling in reference_names.items()
                      if spelling == CONTEXT_NAME or
                      (isinstance(node, ast.Constant) and CONTEXT_NAME in spelling
                       and re.search(r"\bFetchRunContext\b", spelling) is not None)]
        candidates.sort(key=lambda node: _source_order(node, parents, children))
        for node in candidates:
            literal = (isinstance(node, ast.Constant) and isinstance(node.value, str)
                       and CONTEXT_NAME in node.value
                       and re.search(r"\bFetchRunContext\b", node.value) is not None)
            reference = reference_names.get(node) == CONTEXT_NAME or literal
            if reference:
                owner = _qualified_owner(node, parents)
                parent = parents.get(node)
                grandparent = parents.get(parent)
                imported = (isinstance(node, ast.alias) and node.name == CONTEXT_NAME
                            and isinstance(parent, ast.ImportFrom)
                            and parent.module == "fetch_run_context" and node.asname is None)
                create = (isinstance(node, ast.Name) and node.id == CONTEXT_NAME
                          and isinstance(parent, ast.Attribute) and parent.attr == "create"
                          and isinstance(grandparent, ast.Call) and grandparent.func is parent)
                direct = isinstance(parent, ast.Call) and parent.func is node
                type_check = (isinstance(node, ast.Name) and isinstance(parent, ast.Call)
                              and isinstance(parent.func, ast.Name) and parent.func.id == "isinstance"
                              and len(parent.args) == 2 and parent.args[1] is node)
                annotation = ((isinstance(parent, ast.AnnAssign) and parent.annotation is node)
                              or (isinstance(parent, ast.arg) and parent.annotation is node))
                allowed = offline
                if path == "engine/fetch_operations.py" and owner == "begin_operation":
                    allowed = imported or create
                elif path == "engine/stock_ibkr.py" and owner == "":
                    allowed = imported  # Existing unused import is not a constructor.
                elif path == "engine/fetch_run_context.py":
                    allowed = ((owner in {"FetchWorker", "filter_rows",
                                           "unfiltered_diagnostic_evidence"} and annotation)
                               or (owner == "LogicalRequest._execute" and (type_check or literal)))
                elif path == "engine/fetch_operation_inventory.py":
                    # This scanner necessarily names its subject in patterns and
                    # explanations. Never exempt its executable references.
                    allowed = literal
                if not allowed:
                    raise ValueError("unregistered context reference: " + path + ":" + owner)
                kind = ("import" if imported else "create" if create else "direct" if direct
                        else "literal" if literal else "read")
                references.append({"path": path, "owner": owner, "kind": kind})
                if create or direct:
                    constructors.append({"path": path, "owner": owner, "kind": kind})
    production = [row for row in constructors if row["path"] not in CONTEXT_TEST_OWNERS]
    if production != [{"path": "engine/fetch_operations.py", "owner": "begin_operation", "kind": "create"}]:
        raise ValueError("production context constructor denominator changed")
    key = lambda row: (row["path"], row["owner"], row["kind"])
    return {"offline_owners": sorted(CONTEXT_TEST_OWNERS),
            "constructors": sorted(constructors, key=key),
            "references": sorted(references, key=key)}


def build_inventory(sources=None):
    if sources is None:
        sources = source_catalog()
    indexes = {}
    contexts = _context_inventory_indexes(_indexed_sources(sources, indexes))
    sites, production, dispatches, forwarded, metadata = [], [], [], [], []
    for path, indexed in indexes.items():
        if isinstance(indexed, bool):  # Compacted registered-test mint fact.
            if indexed:
                raise ValueError("unregistered operation mint reference: " + path)
            continue
        tree, nodes, parents, children, reference_names = indexed
        # The widened catalog adds construction custody, not permission for
        # archived tools/reviewer code to become operation roots.
        if "/" in path and path.split("/", 1)[0] not in {"engine", "tools", "ops"}:
            continue
        if path in {"engine/fetch_operations.py", "engine/fetch_operation_inventory.py"}:
            continue
        is_test = _test_source(path)
        fix_modules = {"fix_data_pipeline"} | {
            node.asname for node in nodes if isinstance(node, ast.alias)
            and node.name == "fix_data_pipeline" and node.asname}
        references = not APIS.isdisjoint(reference_names.values())
        root_modules = {module for module, owner in ROOT_OWNERS}
        if references and path not in root_modules | {TEST_OWNER}:
            raise ValueError("unregistered operation mint reference: " + path)
        for node in nodes:
            if node not in reference_names and not isinstance(node, ast.ImportFrom):
                continue  # No mint/root/module reference check applies.
            parent = parents.get(node)
            direct_call = isinstance(parent, ast.Call) and parent.func is node
            # `run` is qualified by module, never added to the global spelling
            # set: subprocess.run and unrelated schedulers are not repair roots.
            if not is_test:
                if (isinstance(node, ast.ImportFrom) and node.module == "fix_data_pipeline"
                        and any(item.name in {"run", "*"} for item in node.names)):
                    raise ValueError("indirect Fix Data root import: " + path)
                if (isinstance(node, ast.Name) and node.id in fix_modules
                        and not (isinstance(parent, ast.Attribute) and parent.value is node)):
                    raise ValueError("indirect Fix Data module reference: " + path)
                if (isinstance(node, ast.Attribute) and node.attr == "run"
                        and isinstance(node.value, ast.Name) and node.value.id in fix_modules
                        and not direct_call):
                    raise ValueError("indirect Fix Data root dispatch: " + path)
            if path in root_modules and reference_names.get(node) in APIS:
                if not (direct_call and isinstance(node, ast.Attribute)
                        and node.attr == "begin_operation"
                        and isinstance(node.value, ast.Name) and node.value.id == "fops"):
                    raise ValueError("aliased or indirect production mint reference")
            if not is_test and reference_names.get(node) in ROOT_API_NAMES:
                if _path_metadata(node, parents, path) is not None:
                    continue
                if (isinstance(parent, ast.keyword) and parent.arg == "fill_fn"
                        and isinstance(node, ast.Attribute) and _dotted(node) == "stock_ibkr.fill_missing_days"):
                    continue  # Exact owner and receiving call checked below.
                if not (direct_call and (isinstance(node, ast.Name)
                        or (isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name)
                            and node.value.id in {"stock_ibkr", "sk", "probe",
                                                  "stock_validate", "sv", "validate",
                                                  "sweep", "external_sweep"}))):
                    raise ValueError("unregistered or indirect root dispatch: " + path)
        if not is_test or path == TEST_OWNER:
            def visit(node, names=()):
                parent = parents.get(node)
                if not is_test:
                    meta = _path_metadata(node, parents, path)
                    if meta is not None:
                        metadata.append({"path": path, "producer": meta, "api": node.value})
                    if isinstance(parent, ast.keyword) and reference_names.get(node) in ROOT_PURPOSES:
                        owner = ".".join(names)
                        target = parents.get(parent)
                        key = (path, owner, getattr(node, "attr", None), parent.arg,
                               _dotted(target.func) if isinstance(target, ast.Call) else None)
                        if key not in FORWARDED_REFERENCES:
                            raise ValueError("unregistered forwarded root reference owner: " + path)
                        forwarded.append({"path": path, "owner": owner, "api": key[2],
                            "keyword": key[3], "receiver": key[4], "mode": "held_callback"})
                    if (reference_names.get(node) in TRANSITIVE_APIS and not isinstance(node, ast.Constant)
                            and not (isinstance(parent, ast.Call) and parent.func is node
                                and _dotted(node) in {"preflight", "stock_ibkr.preflight", "sk.preflight"})):
                        raise ValueError("unregistered or indirect transitive dispatch: " + path)
                if isinstance(node, ast.Call):
                    name = getattr(node.func, "attr", getattr(node.func, "id", None))
                    if name == "begin_operation":
                        owner = ".".join(names)
                        if path == TEST_OWNER:
                            sites.append({"path": path, "owner": owner, "api": name})
                        else:
                            purpose = node.args[0].value if node.args and isinstance(node.args[0], ast.Constant) else None
                            if ((path, owner) not in ROOT_OWNERS
                                    or purpose != ROOT_OWNERS[(path, owner)]
                                    or not isinstance(node.func, ast.Attribute)
                                    or not isinstance(node.func.value, ast.Name) or node.func.value.id != "fops"):
                                raise ValueError("unregistered root owner or nonliteral purpose")
                            production.append({"path": path, "owner": owner, "purpose": purpose, "api": name})
                    fix_dispatch = (isinstance(node.func, ast.Attribute) and name == "run"
                        and isinstance(node.func.value, ast.Name) and node.func.value.id in fix_modules)
                    if not is_test and (name in DISPATCH_PURPOSES or fix_dispatch):
                        owner = ".".join(names)
                        receiver = (node.func.value.id if isinstance(node.func, ast.Attribute)
                                    and isinstance(node.func.value, ast.Name) else None)
                        allowed = ([("display_data.py", owner, api, "stock_ibkr")
                                    for owner, api in GUI_DISPATCHES] +
                                   [("display_data.py", owner, api, receiver)
                                    for owner, api, receiver in GUI_HTTP_DISPATCHES] +
                                   ENGINE_DISPATCHES)
                        if (path, ".".join(names), name, receiver) not in allowed:
                            raise ValueError("unregistered root dispatch owner: "
                                             + repr((path, owner, name, receiver)))
                        child = any(keyword.arg == "_fetch_child" for keyword in node.keywords)
                        if child != ((path, owner, name) in CHILD_DISPATCHES):
                            raise ValueError("root dispatch child mode changed: " + path + ":" + owner)
                        purpose = "repair" if fix_dispatch else DISPATCH_PURPOSES[name]
                        dispatches.append({"path": path, "owner": ".".join(names),
                            "api": name, "purpose": "inherited" if child else purpose,
                            "standalone_purpose": purpose,
                            "mode": "child" if child else "transitive" if name in TRANSITIVE_APIS else "root"})
            # Every effective visitor branch consumes one of these spellings
            # (including literal metadata), or its immediate direct Call.
            # Sorting by the original tree preorder preserves competing
            # refusal precedence; parent chains retain qualified owners.
            spellings = (set(ROOT_PURPOSES) | set(TRANSITIVE_APIS)
                         | set(DISPATCH_PURPOSES) | {"begin_operation", "run"})
            candidates = set()
            for node, spelling in reference_names.items():
                if spelling not in spellings:
                    continue
                candidates.add(node)
                parent = parents.get(node)
                if isinstance(parent, ast.Call) and parent.func is node:
                    candidates.add(parent)
            for node in sorted(candidates, key=lambda node: _source_order(node, parents, children)):
                owner = _qualified_owner(node, parents)
                visit(node, tuple(owner.split(".")) if owner else ())
    expected = [{"path": TEST_OWNER, "owner": "Operations.open", "api": "begin_operation"}]
    if sites != expected:
        raise ValueError("operation mint site denominator changed")
    expected_roots = [{"path": path, "owner": name, "purpose": purpose,
                       "api": "begin_operation"} for (path, name), purpose in sorted(ROOT_OWNERS.items())]
    production.sort(key=lambda row: (row["path"], row["owner"]))
    if production != expected_roots:
        raise ValueError("production root denominator changed")
    dispatches.sort(key=lambda row: (row["path"], row["owner"], row["api"]))
    expected_dispatches = sorted([("display_data.py", owner, api) for owner, api in GUI_DISPATCHES]
        + [("display_data.py", owner, api) for owner, api, _ in GUI_HTTP_DISPATCHES]
        + [(path, owner, api) for path, owner, api, _ in ENGINE_DISPATCHES])
    if [(row["path"], row["owner"], row["api"]) for row in dispatches] != expected_dispatches:
        raise ValueError("production root dispatch denominator changed")
    if sorted((row["path"], row["owner"], row["api"], row["keyword"], row["receiver"])
              for row in forwarded) != sorted(FORWARDED_REFERENCES):
        raise ValueError("forwarded root reference denominator changed")
    expected_metadata = [{"path": "engine/fetch_send_inventory.py", "producer": producer,
                          "api": "fill_missing_days"} for producer in
                         ("ibkr.gap_fill.month_daily", "ibkr.gap_fill.month_intraday")]
    if metadata != expected_metadata:
        raise ValueError("root call-path metadata denominator changed")
    return {"schema_version": 1, "phase": "a2-2d-in-progress",
            "production_roots": production, "production_dispatches": dispatches,
            "forwarded_references": forwarded, "call_path_metadata": metadata,
            "test_mint_sites": sites, "context_custody": contexts}


def check():
    actual = build_inventory()
    if json.loads(ARTIFACT.read_text(encoding="utf-8")) != actual:
        raise ValueError("operation inventory differs from committed artifact")
    return actual


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--write", action="store_true", help="regenerate the reviewed source artifact")
    args = parser.parse_args()
    if args.write:
        ARTIFACT.write_text(json.dumps(build_inventory(), indent=2) + "\n", encoding="utf-8", newline="\n")
    print(json.dumps(check(), sort_keys=True))
