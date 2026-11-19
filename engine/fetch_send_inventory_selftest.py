"""Offline contract tests for the Row 79 A1 send inventory."""

from __future__ import annotations

import ast
import contextlib
import copy
import io
import inspect
import hashlib
import json
from pathlib import Path
import sys
import tempfile
from functools import lru_cache
from unittest.mock import Mock, patch


ENGINE_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ENGINE_ROOT))

import check_kit  # noqa: E402
import fetch_send_inventory as inv  # noqa: E402


KIT = check_kit.CheckKit()
check = KIT.check
section = KIT.section
EXPECTED_NON_REQ_METHODS = (
    "qualifyContracts", "qualifyContractsAsync", "connect", "connectAsync",
    "managedAccounts", "placeOrder", "cancelOrder", "whatIfOrder", "whatIfOrderAsync",
)


def expect_error(name, text, function):
    try:
        function()
    except inv.InventoryError as exc:
        check(name, text in str(exc), str(exc))
        return
    except BaseException as exc:  # noqa: BLE001 - exact fail-closed oracle
        check(name, False, f"wrong {type(exc).__name__}: {exc}")
        return
    check(name, False, "did not raise")


def synthetic_classifications(rows):
    result = {}
    for row in rows:
        primitive = str(row["primitive"])
        result[str(row["site_key"])] = inv._site(
            f"synthetic.{primitive}",
            "ibkr-bars" if primitive != "ibkr_raw_head" else "ibkr-head",
            "guarded_choke_point" if primitive.startswith("ibkr_raw_")
            else "a2_refused",
            "synthetic fixture",
        )
    return result


section("committed denominator")
inventory = inv.build_inventory()
sites = inventory["sites"]
check("committed artifact is exact deterministic regeneration",
      inv.INVENTORY_PATH.read_bytes() == inv.render_inventory(inventory))
check("candidate denominator is 71 after the exact held legacy split-sender retirement",
      inventory["candidate_count"] == 71,
      str(inventory["candidate_count"]))
check("logical producer denominator is 60 including the owned split physical sender",
      inventory["logical_producer_count"] == 60,
      str(inventory["logical_producer_count"]))
check("25 syntactic adapter fetch sites are inventoried",
      inventory["primitive_counts"].get("adapter_fetch") == 25,
      repr(inventory["primitive_counts"]))
check("two branch pairs plus coverage-edge probe account for the 25-to-22 adapter delta",
      sum(1 for row in sites if row["primitive"] == "adapter_fetch")
      - len({row["producer_id"] for row in sites
             if row["primitive"] == "adapter_fetch"}) == 3)
check("six adapter head sites cover three logical producers",
      inventory["primitive_counts"].get("adapter_head") == 6
      and len({row["producer_id"] for row in sites
               if row["primitive"] == "adapter_head"}) == 3,
      repr(inventory["primitive_counts"]))
check("three remaining raw HTTP sites include the diagnostic control probe",
      inventory["primitive_counts"].get("http_urlopen") == 3,
      repr(inventory["primitive_counts"]))
check("three additional raw IB requests are inventoried",
      inventory["primitive_counts"].get("ibkr_raw_request") == 3,
      repr(inventory["primitive_counts"]))
check("22 non-req candidates are explicitly classified, including two HTTP connect helpers",
      inventory["primitive_counts"].get("ibkr_non_req") == 22)
check("non-req method policy includes every required sync/async variant",
      inv.IBKR_NON_REQ_METHODS == frozenset(EXPECTED_NON_REQ_METHODS))
check("one logical provider dispatch is separately visible",
      inventory["primitive_counts"].get("provider_fetch") == 1)
check("five sockets include four controls and the held HTTP socket",
      inventory["primitive_counts"].get("socket") == 5)
check("owned physical HTTP open and fixed-host DNS are explicit",
      inventory["primitive_counts"].get("http_opener_open") == 3
      and inventory["primitive_counts"].get("socket_dns") == 1)
check("candidate surface digest is canonical lowercase SHA-256",
      len(inventory["candidate_surface_sha256"]) == 64
      and inventory["candidate_surface_sha256"].isalnum()
      and inventory["candidate_surface_sha256"].lower()
      == inventory["candidate_surface_sha256"])
check("A2-3c exact finite membership and source-line surface digest is pinned",
      inventory["candidate_surface_sha256"]
      == "6f41c00443b59ca73d9399fcc03e6d36e271e6eb326d143590320e209f96a802")
check("artifact contains no project absolute path",
      str(ENGINE_ROOT.parent).casefold() not in
      inv.render_inventory(inventory).decode("utf-8").casefold())

section("single raw choke points")
check("one raw historical bar send is pinned",
      inventory["raw_choke_points"]["ibkr_bars"]
      == "engine.stock_ibkr:LiveIB.fetch.transport:ibkr_raw_bars:1")
check("one raw head send is pinned",
      inventory["raw_choke_points"]["ibkr_head"]
      == "engine.stock_ibkr:LiveIB.head_timestamp:ibkr_raw_head:1")
raw_sites = [row for row in sites
             if row["primitive"] in {"ibkr_raw_bars", "ibkr_raw_head"}]
check("both raw sites are guarded choke points",
      len(raw_sites) == 2
      and all(row["disposition"] == "guarded_choke_point"
              for row in raw_sites))
check("raw sites remain inside LiveIB methods",
      {row["qualname"] for row in raw_sites}
      == {"LiveIB.fetch.transport", "LiveIB.head_timestamp"})

section("A1 path and A2 exclusion")
a1_sites = [row for row in sites
            if row["disposition"] == "a1_context_required"]
check("A1 has eleven syntactic context-required sites", len(a1_sites) == 11,
      str(len(a1_sites)))
check("A1 has exactly six logical producers",
      len({row["producer_id"] for row in a1_sites}) == 6
      and {row["producer_id"] for row in a1_sites} == set(inv.A1_CALL_PATHS))
check("every A1 site is an IBKR bar or head site in stock_ibkr",
      all(row["module"] == "engine.stock_ibkr"
          and (row["primitive"], row["transport"]) in {
              ("adapter_fetch", "ibkr-bars"), ("adapter_head", "ibkr-head")
          } for row in a1_sites))
check("every A1 call path starts at gap_fill_parallel",
      all(row["a1_call_path"][0] == "gap_fill_parallel" for row in a1_sites))
check("every A1 call path ends at its lexical send owner",
      all(row["a1_call_path"][-1] == row["qualname"] for row in a1_sites))
check("non-A1 adapter fetches are all A2-refused",
      all(row["disposition"] in {"a1_context_required", "a2_refused"}
          for row in sites if row["primitive"] == "adapter_fetch"))
http_sites = [row for row in sites
              if row["transport"] in {"http-series", "http-metadata", "http-mixed"}]
check("all six remaining HTTP data/provider sites remain A2-refused",
      len(http_sites) == 6
      and all(row["disposition"] == "a2_refused" for row in http_sites))
stockanalysis_sites = {
    row["site_key"]: (row["line"], row["call"], row["disposition"])
    for row in http_sites if row["producer_id"].startswith("http.stockanalysis.")
}
expected_stockanalysis = {
    "engine.external_sweep:StockAnalysisProvider._read:http_urlopen:1":
        (624, "urllib.request.urlopen(request, timeout=self.timeout)", "a2_refused"),
    "engine.external_sweep:_provider_fetch:provider_fetch:1":
        (1044, "provider.fetch(symbol, **options)", "a2_refused"),
    "engine.stock_validate:fetch_daily_reference:http_urlopen:1":
        (182, "urllib.request.urlopen(req, timeout=20)", "a2_refused"),
    "engine.fetch_http_labels:guarded_stockanalysis_attempt.one_send:http_opener_open:1":
        (153, "urllib.request.OpenerDirector.open( opener, wire, timeout=deadline.remaining())", "a2_refused"),
}
check("A2-3a four StockAnalysis sites include the exact owned physical open",
      stockanalysis_sites == expected_stockanalysis,
      repr(stockanalysis_sites))
for key, value in expected_stockanalysis.items():
    mutant = dict(stockanalysis_sites)
    mutant[key] = (value[0] + 1, value[1], value[2])
    check("A2-3a source-line inverse is red: " + key,
          mutant != expected_stockanalysis)
membership_sites = {
    row['site_key']: (row['line'], row['call'], row['disposition'])
    for row in http_sites if row['module'] == 'engine.fetch_http_membership'
}
expected_membership = {
    'engine.fetch_http_membership:guarded_membership_attempt.one_send:http_opener_open:1':
        (143, 'urllib.request.OpenerDirector.open(opener, wire, timeout=deadline.remaining())', 'a2_refused'),
}
check('A2-3c finite membership has exactly one owned default-held physical site',
      membership_sites == expected_membership, repr(membership_sites))
key = next(iter(expected_membership))
line, call, disposition = expected_membership[key]
for label, changed in (('line', (line + 1, call, disposition)),
                       ('call', (line, call.replace('deadline.remaining()', '120'), disposition)),
                       ('permissive', (line, call, 'guarded_choke_point'))):
    check('A2-3c membership exact binding inverse RED: ' + label,
          {key: changed} != expected_membership)
membership_source = (ENGINE_ROOT / 'fetch_http_membership.py').read_text(encoding='utf-8')
original_membership_primitive = inv._primitive
with patch.object(inv, '_primitive', side_effect=lambda node, aliases:
        None if original_membership_primitive(node, aliases) == 'http_opener_open'
        else original_membership_primitive(node, aliases)):
    check('A2-3c actual membership scanner removal inverse RED',
          not any(row['primitive'] == 'http_opener_open' for row in
                  inv.scan_source(membership_source, 'engine.fetch_http_membership')))
embedded_sites = {
    row["site_key"]: (row["line"], row["call"], row["disposition"])
    for row in sites if row["module"] in {
        "engine.live_combined_flags", "engine.live_validate_catchup"}
}
expected_embedded = {
    "engine.live_combined_flags:_connect:ibkr_non_req:1":
        (258, "sk.LiveIB(host=sk.HOST_DEFAULT, ports=(port,), client_id=sk.CLIENT_ID_FETCH).connect()",
         "control_message"),
    "engine.live_combined_flags:_fetch_month_1d:adapter_fetch:1":
        (116, 'adapter.fetch(contract, end.replace(tzinfo=None), "2 M", "1 day", what_to_show="TRADES")',
         "a2_refused"),
    "engine.live_combined_flags:_fetch_month_1m:adapter_fetch:1":
        (100, 'adapter.fetch(contract, end.replace(tzinfo=None), "1 M", "1 min", what_to_show="TRADES")',
         "a2_refused"),
    "engine.live_validate_catchup:_catchup_port:ibkr_non_req:1":
        (104, "sk.LiveIB(host=sk.HOST_DEFAULT, ports=(p,), client_id=sk.CLIENT_ID_FETCH).connect()",
         "control_message"),
}
check("A2-3a four source-moved embedded sites are exact pinned",
      embedded_sites == expected_embedded, repr(embedded_sites))
for key, value in expected_embedded.items():
    mutant = dict(embedded_sites)
    mutant[key] = (value[0] + 1, value[1], value[2])
    check("A2-3a embedded source-line inverse is red: " + key,
          mutant != expected_embedded)
socket_sites = [row for row in sites if row["primitive"] == "socket"]
control_sockets = [row for row in socket_sites if row["producer_id"] != "http.stockanalysis.connection.socket"]
check("four control sockets remain distinct from the held HTTP connection socket",
      len(socket_sites) == 5 and len(control_sockets) == 4
      and all(row["disposition"] == "control_socket"
              and row["transport"] == "socket-control"
              for row in control_sockets)
      and next(row for row in socket_sites if row["producer_id"] ==
               "http.stockanalysis.connection.socket")["disposition"] == "a2_refused")
head_sites = [row for row in sites if row["primitive"] == "adapter_head"]
check("nightly head branch pair has an explicit backfill path",
      len([row for row in head_sites
           if row["qualname"] == "_head_start_evidence"]) == 2
      and all(row["a1_call_path"][-3:] == [
          "_fill_series_inner", "_backfill_earlier", "_head_start_evidence"]
          for row in head_sites if row["qualname"] == "_head_start_evidence"))
check("post-series earliest head branch pair has the real nightly path",
      len([row for row in head_sites if row["qualname"] == "earliest_available"]) == 2
      and all(row["disposition"] == "a1_context_required"
              and row["a1_call_path"] == ["gap_fill_parallel",
                  "gap_fill_parallel.worker", "gap_fill", "earliest_available"]
              for row in head_sites if row["qualname"] == "earliest_available"))
check("only two identity head sites remain A2-refused",
      {row["site_key"] for row in head_sites
       if row["disposition"] == "a2_refused"} == {
          f"engine.stock_ibkr:{owner}:adapter_head:{index}"
          for owner in ("_identity_earliest_evidence",)
          for index in (1, 2)
      })
metadata_sites = [row for row in sites if row["transport"] == "ibkr-metadata"]
check("contract details and symbol search are explicit A2 metadata sends",
      {row["qualname"] for row in metadata_sites if row["disposition"] == "a2_refused"}
      == {"LiveIB.company_name.transport", "LiveIB.search"}
      and len(metadata_sites) == 5)
qualification_sites = [row for row in metadata_sites
                       if row["primitive"] == "ibkr_non_req"]
check("three qualification sends are two shared A1-gated metadata choke points",
      len(qualification_sites) == 3
      and {row["producer_id"] for row in qualification_sites}
      == {"ibkr.choke.qualify", "ibkr.choke.qualify_many"}
      and all(row["disposition"] == "guarded_choke_point"
              and "A1-bound context" in row["reason"] for row in qualification_sites))
check("market-data type selection is an explicit IB control message",
      [(row["qualname"], row["transport"]) for row in sites
       if row["disposition"] == "control_message" and row["primitive"] == "ibkr_raw_request"]
      == [("LiveIB._connect_under_gate", "ibkr-control")])
non_req_controls = [row for row in sites if row["primitive"] == "ibkr_non_req"
                    and row["transport"] not in {"ibkr-metadata", "http-control"}]
check("17 non-req control sites include 15 connection/account and two socket calls",
      len(non_req_controls) == 17
      and sum(row["disposition"] == "control_message" for row in non_req_controls) == 15
      and {row["producer_id"] for row in non_req_controls
           if row["disposition"] == "control_socket"}
      == {"socket.row82.live_ports", "socket.tws_launch.port_open"})
check("root PyPI diagnostic is an explicit non-data HTTP control probe",
      [(row["module"], row["qualname"], row["transport"]) for row in sites
       if row["disposition"] == "control_probe"]
      == [("display_data", "print_diagnostics", "http-control")])
check("no non-A1 site carries an A1 call path",
      all("a1_call_path" not in row for row in sites
          if row["disposition"] != "a1_context_required"))
check("disposition counts are pinned",
      inventory["disposition_counts"] == {
          "a1_context_required": 11,
          "a2_refused": 32,
          "control_message": 16,
          "control_probe": 1,
          "control_socket": 6,
          "guarded_choke_point": 5,
      }, repr(inventory["disposition_counts"]))

section("fail-closed regenerate-and-diff mutations")
base_source = """\
def send(adapter, ib):
    adapter.fetch('contract', 'end', '1 D', '1 day')
    ib.reqHistoricalDataAsync('contract')
    ib.reqHeadTimeStampAsync('contract')
"""
with tempfile.TemporaryDirectory(prefix="fetch-inventory-") as tmp:
    root = Path(tmp) / "engine"
    root.mkdir()
    source_path = root / "synthetic.py"
    source_path.write_text(base_source, encoding="utf-8")
    base_rows = inv.scan_tree(root)
    classifications = synthetic_classifications(base_rows)
    baseline = inv.build_inventory(root, classifications)
    baseline_bytes = inv.render_inventory(baseline)

    source_path.write_text("\n\n" + base_source, encoding="utf-8")
    moved_rows = inv.scan_tree(root)
    moved = inv.build_inventory(root, classifications)
    check("line moves preserve stable site keys",
          {row["site_key"] for row in base_rows}
          == {row["site_key"] for row in moved_rows})
    check("line moves change the committed inventory bytes",
          inv.render_inventory(moved) != baseline_bytes)

    source_path.write_text(base_source + "\ndef harmless():\n    return 1\n",
                           encoding="utf-8")
    harmless = inv.build_inventory(root, classifications)
    check("unrelated source outside the send surface needs no re-pin",
          inv.render_inventory(harmless) == baseline_bytes)

    source_path.write_text(
        base_source + "\ndef new_http():\n    return urlopen('https://invalid')\n",
        encoding="utf-8")
    expect_error("new unclassified network primitive fails closed",
                 "unclassified=", lambda: inv.build_inventory(root, classifications))

    for call, primitive in (
            ("urllib.request.OpenerDirector.open(opener, request)", "http_opener_open"),
            ("socket.getaddrinfo('stockanalysis.com', 443)", "socket_dns")):
        source_path.write_text(
            base_source + "\ndef new_send():\n    " + call + "\n",
            encoding="utf-8")
        rows = inv.scan_tree(root)
        check("new default-route primitive is recognized: " + primitive,
              any(row["primitive"] == primitive for row in rows))
        expect_error("new default-route primitive needs classification: " + primitive,
                     "unclassified=", lambda: inv.build_inventory(root, classifications))
        original_primitive = inv._primitive
        with patch.object(inv, "_primitive", side_effect=lambda node, aliases:
                None if original_primitive(node, aliases) == primitive
                else original_primitive(node, aliases)):
            check("default-route scanner removal inverse is red: " + primitive,
                  not any(row["primitive"] == primitive for row in inv.scan_tree(root)))

    for method in ("head_timestamp", "reqHistoricalData", "reqMktData",
                   "reqHistoricalTicksAsync", "reqFutureTransportAsync",
                   *EXPECTED_NON_REQ_METHODS):
        source_path.write_text(
            base_source + f"\ndef new_send(client):\n    client.{method}()\n",
            encoding="utf-8")
        expect_error(f"new {method} producer fails without classification",
                     "unclassified=",
                     lambda: inv.build_inventory(root, classifications))

    source_path.write_text(
        base_source.replace(
            "    adapter.fetch('contract', 'end', '1 D', '1 day')\n", ""),
        encoding="utf-8")
    expect_error("deleted send site leaves a stale classification",
                 "stale=", lambda: inv.build_inventory(root, classifications))

    source_path.write_text(base_source, encoding="utf-8")
    changed_classifications = {
        key: dict(value) for key, value in classifications.items()
    }
    first_key = sorted(changed_classifications)[0]
    changed_classifications[first_key]["reason"] = "reviewed classification changed"
    reclassified = inv.build_inventory(root, changed_classifications)
    check("reclassification changes the committed inventory bytes",
          inv.render_inventory(reclassified) != baseline_bytes)

    missing = dict(classifications)
    missing.pop(sorted(missing)[0])
    expect_error("missing reviewed classification fails closed",
                 "unclassified=", lambda: inv.build_inventory(root, missing))

    stale = dict(classifications)
    stale["engine.synthetic:ghost:socket:1"] = inv._site(
        "synthetic.ghost", "socket-control", "control_socket", "ghost")
    expect_error("stale reviewed classification fails closed",
                 "stale=", lambda: inv.build_inventory(root, stale))

    # Each new module must be discovered from the copied project, without Git
    # or an import of executable application/tool code.
    for relative, module in (
            ("diagnostic.py", "diagnostic"),
            ("tools/probe.py", "tools.probe"),
            ("tools/nested/probe.py", "tools.nested.probe"),
            ("ops/probe.py", "ops.probe"),
            ("engine/nested/probe.py", "engine.nested.probe")):
        extra_path = Path(tmp) / relative
        extra_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            extra_path.write_text(
                "import urllib.request\n"
                "def send():\n    urllib.request.urlopen('https://invalid')\n",
                encoding="utf-8")
            expect_error(f"unclassified shipped send in {relative} fails closed",
                         f"{module}:send:http_urlopen:1",
                         lambda: inv.build_inventory(root, classifications))
        finally:
            extra_path.unlink()

    for relative in ("archive/old.py", "docs/fixture.py", ".agents/helper.py",
                     "tools/probe_selftest.py", "ops/probe_reference.py"):
        ignored_path = Path(tmp) / relative
        ignored_path.parent.mkdir(parents=True, exist_ok=True)
        ignored_path.write_text("ignored, not valid Python!\n", encoding="utf-8")
    check("archive/docs/agent sources and excluded harnesses do not enter scope",
          inv.render_inventory(inv.build_inventory(root, classifications))
          == baseline_bytes)

section("negative nightly reachability proof")
reclassified_sites = [dict(row) for row in sites]
for row in reclassified_sites:
    if row["qualname"] == "earliest_available":
        row["disposition"] = "a2_refused"
expect_error("earliest-available A2 misclassification is rejected by reachability",
             "A2-refused owners reachable",
             lambda: inv._a1_paths(ENGINE_ROOT, reclassified_sites))

graph_fixture = """\
class Adapter:
    def unused(self, adapter):
        adapter.fetch('contract')
def forbidden(adapter):
    adapter.head_timestamp('contract')
def bridge(adapter):
    forbidden(adapter)
def gap_fill_parallel():
    Adapter()
def raw(ib):
    ib.reqHistoricalDataAsync('contract')
    ib.reqHeadTimeStampAsync('contract')
"""
with tempfile.TemporaryDirectory(prefix="fetch-inventory-graph-") as tmp:
    graph_root = Path(tmp) / "engine"
    graph_root.mkdir()
    graph_path = graph_root / "stock_ibkr.py"
    graph_path.write_text(graph_fixture, encoding="utf-8")
    graph_classes = synthetic_classifications(inv.scan_tree(graph_root))
    class_only = inv.build_inventory(graph_root, graph_classes)
    check("class construction does not mark every method reachable",
          class_only["candidate_count"] == 4)
    for label, body in (
            ("direct", "forbidden(None)"),
            ("transitive", "bridge(None)"),
            ("nested-function", "def worker():\n        bridge(None)\n    worker()"),
            ("cycle", "gap_fill_parallel()\n    bridge(None)")):
        graph_path.write_text(graph_fixture.replace("Adapter()", body), encoding="utf-8")
        expect_error(f"{label} reachability rejects A2 even with no A1-classified sites",
                     "A2-refused owners reachable",
                     lambda: inv.build_inventory(graph_root, graph_classes))
    graph = inv._LocalCallGraph()
    graph.visit(ast.parse(graph_fixture.replace("Adapter()", "bridge(None)")))
    check("class-to-method edges are absent", ("Adapter", "Adapter.unused") not in graph.edges)
    other_module_sites = [dict(row, module="engine.other")
                          for row in class_only["sites"]]
    inv._check_a1_exclusion(graph, other_module_sites)
    check("local proof does not conflate same-named owners in another module", True)

section("A1-1 O1 residual attribute and imported-module exclusion")
# These calls add no network primitive to the denominator: the residual check
# must reject them even when regeneration/classifications otherwise match.
with tempfile.TemporaryDirectory(prefix="fetch-inventory-residual-") as tmp:
    root = Path(tmp) / "engine"
    root.mkdir()
    path = root / "stock_ibkr.py"
    external = root / "sender.py"
    external.write_text("def send():\n    urlopen('https://invalid')\n", encoding="utf-8")
    path.write_text(graph_fixture, encoding="utf-8")
    classes = synthetic_classifications(inv.scan_tree(root))
    check("residual baseline contains send module but no reachable call",
          inv.build_inventory(root, classes)["candidate_count"] == 5)
    residual_forms = (
        ("attribute details", "", "adapter.company_name('T')"),
        ("attribute search", "", "adapter.search('T')"),
        ("factory receiver", "", "factory().search('T')"),
        ("literal getattr", "", "getattr(adapter, 'company_name')('T')"),
        ("bare module", "import sender\n", "sender.pure_helper()"),
        ("module alias", "import sender as remote\n", "remote.pure_helper()"),
        ("package module", "import engine.sender\n", "engine.sender.pure_helper()"),
        ("package alias", "import engine.sender as remote\n", "remote.pure_helper()"),
        ("from function", "from sender import pure_helper as helper\n", "helper()"),
        ("from module", "from engine import sender as remote\n", "remote.pure_helper()"),
        ("relative function", "from .sender import pure_helper as helper\n", "helper()"),
        ("relative module", "from . import sender as remote\n", "remote.pure_helper()"),
        ("nested import", "", "import sender as remote\n    remote.pure_helper()"),
        ("wildcard", "from sender import *\n", "pure_helper()"),
        ("shadow cannot erase prior import", "import sender as remote\nimport math as remote\n", "remote.pure_helper()"),
        ("transitive helper", "import sender\ndef bridge_import():\n    sender.pure_helper()\n", "bridge_import()"),
    )
    for label, imports, call in residual_forms:
        path.write_text(imports + graph_fixture.replace("Adapter()", call), encoding="utf-8")
        expect_error("residual blocks " + label, "A1 residual exclusion violated",
                     lambda: inv.build_inventory(root, classes))
    path.write_text("import sender\n" + graph_fixture
                    + "\ndef unused_import():\n    sender.pure_helper()\n", encoding="utf-8")
    check("unreachable imported call is not conflated with nightly",
          inv.build_inventory(root, classes)["candidate_count"] == 5)
    path.write_text("import math\n" + graph_fixture.replace("Adapter()", "math.sqrt(4)"), encoding="utf-8")
    check("resolved module without send sites remains permitted",
          inv.build_inventory(root, classes)["candidate_count"] == 5)
    path.write_text("import sender\n" + graph_fixture.replace("Adapter()", "sender.pure_helper()"), encoding="utf-8")
    source = inspect.getsource(inv._check_a1_exclusion)
    needle = "_check_a1_residual(graph, sites, reachable, offline_helpers)"
    check("residual inverse source anchored", source.count(needle) == 1)
    namespace = dict(vars(inv))
    exec(compile(source.replace(needle, "pass"), "<residual-bypass>", "exec"), namespace)
    with patch.object(inv, "_check_a1_exclusion", namespace["_check_a1_exclusion"]):
        check("removing residual pin makes forbidden-call oracle RED",
              inv.build_inventory(root, classes)["candidate_count"] == 5)

section("offline-helper exception custody, never a module-wide bypass")
with tempfile.TemporaryDirectory(prefix="fetch-inventory-helper-") as tmp:
    root = Path(tmp)
    path = root / "stock_validate.py"
    original = (ENGINE_ROOT / "stock_validate.py").read_text(encoding="utf-8")
    helper_sites = [{"module": "engine.stock_validate"}]
    path.write_text(original, encoding="utf-8")
    allowed = inv._offline_helper_exceptions(root, helper_sites)
    check("only two exact helper names in bare/package forms are exempt", allowed == frozenset({
          "engine.stock_validate.load_ibkr_earliest", "engine.stock_validate.record_ibkr_earliest",
          "stock_validate.load_ibkr_earliest", "stock_validate.record_ibkr_earliest"}))
    check("artifact exposes reviewed helper source fingerprints",
          inventory["a1_offline_helper_sha256"] == inv.A1_OFFLINE_HELPER_SHA256)
    for target in inv.A1_OFFLINE_HELPER_SHA256:
        name = target.rsplit(".", 1)[-1]
        node = next(n for n in ast.parse(original).body if isinstance(n, ast.FunctionDef) and n.name == name)
        body = ast.get_source_segment(original, node)
        altered = body.replace('return {}', 'return {"drift": True}', 1) if name.startswith("load") else body.replace('str(ticker or "")', 'str(ticker or "drift")', 1)
        check("helper mutation anchored " + name, body != altered)
        path.write_text(original.replace(body, altered, 1), encoding="utf-8")
        expect_error("helper body drift fails closed " + name, "offline helper source drift",
                     lambda: inv._offline_helper_exceptions(root, helper_sites))
    path.write_text(original.replace("import stock_storage as ss", "import sender as ss"), encoding="utf-8")
    expect_error("helper imported dependency drift fails closed", "offline helper import drift",
                 lambda: inv._offline_helper_exceptions(root, helper_sites))
    path.write_text(original + "\ndef load_ibkr_earliest(root):\n    return {}\n", encoding="utf-8")
    expect_error("duplicate helper definition fails closed", "offline helper source drift",
                 lambda: inv._offline_helper_exceptions(root, helper_sites))
    path.write_text(original.replace("def load_ibkr_earliest(", "def moved_helper("), encoding="utf-8")
    expect_error("missing helper fails closed", "offline helper source drift",
                 lambda: inv._offline_helper_exceptions(root, helper_sites))
    path.write_text(original, encoding="utf-8", newline="\r\n")
    check("helper custody is EOL-normalized", inv._offline_helper_exceptions(root, helper_sites) == allowed)
    graph = inv._LocalCallGraph()
    graph.visit(ast.parse("import stock_validate as sv\ndef gap_fill_parallel():\n    sv.fetch_daily_reference()\n"))
    expect_error("helper exceptions never admit another function in that module", "A1 residual exclusion violated",
                 lambda: inv._check_a1_exclusion(graph, helper_sites, allowed))
    path.write_text(original.replace("str(ticker or \"\")", "str(ticker or \"drift\")"), encoding="utf-8")
    source = inspect.getsource(inv._offline_helper_exceptions)
    needle = "hexdigest() != expected"
    check("helper custody inverse anchored", source.count(needle) == 1)
    namespace = dict(vars(inv))
    exec(compile(source.replace(needle, "hexdigest() != expected and False"),
                 "<helper-custody-bypass>", "exec"), namespace)
    check("removing helper source pin makes drift oracle RED",
          namespace["_offline_helper_exceptions"](root, helper_sites) == allowed)

section("parser and discovery guards")
for method in EXPECTED_NON_REQ_METHODS:
    non_req_forms = inv.scan_source(
        "def send(alias, factory):\n"
        f"    alias.{method}()\n"
        f"    factory().{method}()\n"
        f"    getattr(alias, '{method}')()\n"
        f"    getattr(factory(), '{method}')()\n",
        "engine.synthetic_non_req",
    )
    check(f"{method} captured on named/factory/literal-getattr receivers",
          [row["primitive"] for row in non_req_forms] == ["ibkr_non_req"] * 4,
          json.dumps(non_req_forms, sort_keys=True))
dynamic = inv.scan_source(
    "def get(opener, req):\n"
    "    return (opener or urllib.request.urlopen)(req)\\\n"
    "        .read().decode('utf-8')\n",
    "engine.synthetic_dynamic",
)
check("dynamic opener form records one raw urlopen, not chained calls",
      len(dynamic) == 1 and dynamic[0]["primitive"] == "http_urlopen",
      json.dumps(dynamic, sort_keys=True))
aliased = inv.scan_source(
    "import urllib.request as webio\n"
    "from socket import create_connection as connect\n"
    "def send(req):\n"
    "    webio.urlopen(req)\n"
    "    connect(('127.0.0.1', 1))\n",
    "engine.synthetic_alias",
)
check("import aliases cannot hide HTTP or socket primitives",
      [row["primitive"] for row in aliased] == ["http_urlopen", "socket"],
      json.dumps(aliased, sort_keys=True))
from_alias = inv.scan_source(
    "from urllib.request import urlopen as open_url\n"
    "def send(req):\n"
    "    open_url(req)\n",
    "engine.synthetic_from_alias",
)
check("from-import aliases cannot hide urlopen",
      len(from_alias) == 1 and from_alias[0]["primitive"] == "http_urlopen",
      json.dumps(from_alias, sort_keys=True))
indirect = inv.scan_source(
    "def send(adapter, ib):\n"
    "    getattr(adapter, 'fetch')('contract')\n"
    "    getattr(adapter, 'head_timestamp')('contract')\n"
    "    getattr(ib, 'reqHistoricalDataAsync')('contract')\n"
    "    getattr(ib, 'reqHeadTimeStampAsync')('contract')\n"
    "    getattr(ib, 'reqMktData')('contract')\n",
    "engine.synthetic_getattr",
)
check("literal getattr cannot hide adapter or raw IBKR sends",
      [row["primitive"] for row in indirect]
      == ["adapter_fetch", "adapter_head", "ibkr_raw_bars", "ibkr_raw_head",
          "ibkr_raw_request"],
      json.dumps(indirect, sort_keys=True))
receiver_forms = inv.scan_source(
    "def send(alias, factory):\n"
    "    alias.head_timestamp('contract')\n"
    "    factory().head_timestamp('contract')\n"
    "    alias.reqMatchingSymbols('symbol')\n"
    "    factory().reqHistoricalData('contract')\n"
    "    getattr(factory(), 'reqHistoricalTicksAsync')('contract')\n"
    "    alias.request('not an IB request name')\n",
    "engine.synthetic_receivers",
)
check("receiver aliases and factories cannot hide head or req-capital methods",
      [row["primitive"] for row in receiver_forms]
      == ["adapter_head", "adapter_head", "ibkr_raw_request",
          "ibkr_raw_request", "ibkr_raw_request"],
      json.dumps(receiver_forms, sort_keys=True))
check("site key ignores leading blank-line movement",
      inv.scan_source("\n\n" + base_source, "engine.synthetic")[0]["site_key"]
      == inv.scan_source(base_source, "engine.synthetic")[0]["site_key"])
expect_error("malformed Python source fails closed", "cannot parse",
             lambda: inv.scan_source("def broken(:\n", "engine.broken"))
check("selftests and references are absent from production inventory",
      all(not str(row["module"]).endswith(("_selftest", "_reference"))
          for row in sites))
check("run_gates is excluded as the battery runner",
      all(row["module"] != "engine.run_gates" for row in sites))


section("excluded Phase0 first-effect hold")
phase0_path = ENGINE_ROOT / "split_provider_reference.py"
phase0_tree = ast.parse(phase0_path.read_text(encoding="utf-8"))
phase0_fetch = next(node for node in phase0_tree.body
                    if isinstance(node, ast.FunctionDef)
                    and node.name == "_fetch_bytes")
phase0_guard = ast.dump(ast.parse("refuse_a2()").body[0], include_attributes=False)
check("Phase0 refusal is the first executable statement",
      ast.dump(phase0_fetch.body[0], include_attributes=False) == phase0_guard)


def phase0_load(tree):
    # Non-main namespace: never invoke the real run_proof or default command.
    namespace = {"__name__": "_held_phase0_fixture", "__file__": str(phase0_path)}
    with contextlib.redirect_stdout(io.StringIO()):
        exec(compile(tree, str(phase0_path), "exec"), namespace)
    return namespace


def phase0_observe(tree, invocation):
    effects = {key: 0 for key in (
        "request", "open", "sleep", "dns", "socket", "tcp", "tls", "process",
        "headers")}

    def counted(key, refuses=True):
        def effect(*args, **kwargs):
            effects[key] += 1
            if refuses:
                raise RuntimeError("Phase0 effect tripwire: " + key)
            return object()
        return effect

    class Headers:
        def __bool__(self):
            effects["headers"] += 1
            return True

        def keys(self):
            effects["headers"] += 1
            return ["User-Agent"]

        def __getitem__(self, key):
            effects["headers"] += 1
            return "offline-fixture"

    with contextlib.ExitStack() as stack:
        for target, key, refuses in (
            ("urllib.request.Request", "request", False),
            ("urllib.request.urlopen", "open", True),
            ("time.sleep", "sleep", False),
            ("socket.getaddrinfo", "dns", True),
            ("socket.socket", "socket", True),
            ("socket.create_connection", "tcp", True),
            ("http.client.HTTPConnection.connect", "tcp", True),
            ("http.client.HTTPSConnection.connect", "tls", True),
            ("subprocess.Popen", "process", True),
            ("os.system", "process", True),
        ):
            stack.enter_context(patch(target, side_effect=counted(key, refuses)))
        stack.enter_context(patch.dict("os.environ", {}, clear=True))
        namespace = phase0_load(tree)
        try:
            invocation(namespace, Headers())
        except namespace["RequestRefused"] as exc:
            refusal = "reviewed Milestone A2" in str(exc)
        except Exception:
            refusal = False
        else:
            refusal = False
    return refusal, effects


phase0_url = "https://query1.finance.yahoo.com/v8/finance/chart/FIXTURE"
phase0_cases = (
    ("default arguments", lambda ns, headers: ns["_fetch_bytes"](phase0_url)),
    ("explicit arguments", lambda ns, headers: ns["_fetch_bytes"](
        phase0_url, headers={"User-Agent": "offline-fixture"}, timeout=1, attempts=1)),
    ("injected headers", lambda ns, headers: ns["_fetch_bytes"](
        phase0_url, headers=headers, timeout=1, attempts=2)),
    ("Yahoo helper", lambda ns, headers: ns["fetch_yahoo_events"]("NVDA", timeout=1)),
    ("SEC helper", lambda ns, headers: ns["fetch_sec_sources"](["NVDA"], timeout=1)),
    ("issuer helper", lambda ns, headers: ns["fetch_ftnt_issuer_evidence"](timeout=1)),
)
for label, invoke in phase0_cases:
    held, effects = phase0_observe(phase0_tree, invoke)
    check("Phase0 " + label + " refuses before every effect",
          held and not any(effects.values()), json.dumps(effects, sort_keys=True))

phase0_removed = copy.deepcopy(phase0_tree)
phase0_mutant_fetch = next(node for node in phase0_removed.body
                           if isinstance(node, ast.FunctionDef)
                           and node.name == "_fetch_bytes")
check("Phase0 statement-removal inverse is anchored",
      ast.dump(phase0_mutant_fetch.body[0], include_attributes=False) == phase0_guard)
phase0_mutant_fetch.body.pop(0)
mutant_held, mutant_effects = phase0_observe(phase0_removed, phase0_cases[0][1])
check("Phase0 removing only the hold exposes counted requests and opens",
      not mutant_held and mutant_effects["request"] == 2
      and mutant_effects["open"] == 2 and mutant_effects["sleep"] == 1
      and all(mutant_effects[key] == 0 for key in ("dns", "socket", "tcp", "tls", "process")),
      json.dumps(mutant_effects, sort_keys=True))
check("Phase0 zero-effect boundary oracle becomes RED without the hold",
      not (mutant_held and not any(mutant_effects.values())))

# Artifact-only fixture: replace run_proof before calling the extracted main.
# The real main/default command and its live proof body are never invoked.
def phase0_artifact(tree, *, attempted=False):
    import fetch_ibkr_bridge
    namespace = {"argparse": __import__("argparse"), "dt": __import__("datetime"),
                 "json": json, "sys": sys, "Path": Path,
                 "RequestRefused": fetch_ibkr_bridge.RequestRefused}

    def refused_proof(*, timeout):
        if attempted:
            raise RuntimeError("counted non-refusal proof stub; no real transport")
        raise fetch_ibkr_bridge.RequestRefused(
            "producer remains disabled until reviewed Milestone A2")

    namespace["run_proof"] = refused_proof
    main_node = next(node for node in tree.body
                     if isinstance(node, ast.FunctionDef) and node.name == "main")
    exec(compile(ast.Module(body=[main_node], type_ignores=[]),
                 "<phase0-artifact-only>", "exec"), namespace)
    output = io.StringIO()
    with contextlib.redirect_stdout(output):
        code = namespace["main"]([])
    return code, json.loads(output.getvalue())


artifact_code, artifact = phase0_artifact(phase0_tree)
check("Phase0 typed pre-send refusal artifact reports no network and exit1",
      artifact_code == 1 and artifact["network"] is False
      and artifact["result"] == "fail" and artifact["bank_writes"] is False
      and artifact["error"].startswith("RequestRefused:"))
artifact_mutant = copy.deepcopy(phase0_tree)
artifact_main = next(node for node in artifact_mutant.body
                     if isinstance(node, ast.FunctionDef) and node.name == "main")
artifact_fields = [node for node in ast.walk(artifact_main) if isinstance(node, ast.Dict)]
artifact_network = [(node, index) for node in artifact_fields
                    for index, key in enumerate(node.keys)
                    if isinstance(key, ast.Constant) and key.value == "network"]
check("Phase0 artifact flag inverse is anchored", len(artifact_network) == 1)
artifact_dict, artifact_index = artifact_network[0]
artifact_dict.values[artifact_index] = ast.copy_location(
    ast.Constant(value=True), artifact_dict.values[artifact_index])
_, bad_artifact = phase0_artifact(artifact_mutant)
check("Phase0 removing refusal classification makes the no-network oracle RED",
      bad_artifact["network"] is not False)

attempted_code, attempted_artifact = phase0_artifact(phase0_tree, attempted=True)
check("Phase0 non-refusal artifact stub conservatively reports network and exit1",
      attempted_code == 1 and attempted_artifact["network"] is True
      and attempted_artifact["result"] == "fail"
      and attempted_artifact["bank_writes"] is False
      and attempted_artifact["error"].startswith("RuntimeError:"))
artifact_false = copy.deepcopy(phase0_tree)
false_main = next(node for node in artifact_false.body
                  if isinstance(node, ast.FunctionDef) and node.name == "main")
false_network = [(node, index) for node in ast.walk(false_main) if isinstance(node, ast.Dict)
                 for index, key in enumerate(node.keys)
                 if isinstance(key, ast.Constant) and key.value == "network"]
check("Phase0 blanket-no-network inverse is independently anchored", len(false_network) == 1)
false_dict, false_index = false_network[0]
false_dict.values[false_index] = ast.copy_location(
    ast.Constant(value=False), false_dict.values[false_index])
_, false_artifact = phase0_artifact(artifact_false, attempted=True)
check("Phase0 blanket no-network mutation makes the non-refusal oracle RED",
      false_artifact["network"] is not True)

phase0_parser_results = {}


def phase0_parse_fixtures(namespace, headers):
    phase0_parser_results["yahoo"] = namespace["parse_yahoo_events"](
        {"chart": {"result": [{"events": {"splits": {}}}]}}, "FIXTURE")
    phase0_parser_results["sec"] = namespace["parse_sec_tickers"](
        {"0": {"ticker": "FIXTURE", "cik_str": 123}})


_, parser_effects = phase0_observe(phase0_tree, phase0_parse_fixtures)
check("Phase0 pure Yahoo parser remains callable without a fetch",
      phase0_parser_results.get("yahoo") == [] and not any(parser_effects.values()))
check("Phase0 pure SEC identity parser remains callable without a fetch",
      phase0_parser_results.get("sec") == {"FIXTURE": 123}
      and not any(parser_effects.values()))




# Separate AST-only custody for send candidates omitted from production scans.
# These exact keys are not permission to run a reference or a live main.
# Whole-source proof hashes are EOL-normalized, deliberately conservative.
_EXCLUDED_SEND_DISPOSITIONS = (
    ("engine.daily_bar_repair_selftest:DailyRepair.test_raw_adapter_call_without_authority_refused:adapter_fetch:1", "engine/daily_bar_repair_selftest.py", "self.adapter.fetch(ib_contract(), datetime(2026, 7, 8, 16), \"1 D\", \"1 day\")", "DAILY_REPAIR_CONFINED_AND_FIXED_CHILDREN"),
    ("engine.daily_bar_repair_selftest:DailyRepair.test_tripwires_are_live:socket:1", "engine/daily_bar_repair_selftest.py", "socket.create_connection((\"127.0.0.1\", 7497))", "DAILY_REPAIR_CONFINED_AND_FIXED_CHILDREN"),
    ("engine.daily_bar_repair_selftest:HardExit.test_process_death_and_fresh_process_recovery:census_process_launch:1", "engine/daily_bar_repair_selftest.py", "subprocess.run(base + [\"--crash-child\", phase, str(parent)], capture_output=True, text=True, timeout=40)", "DAILY_REPAIR_CONFINED_AND_FIXED_CHILDREN"),
    ("engine.daily_bar_repair_selftest:HardExit.test_process_death_and_fresh_process_recovery:census_process_launch:2", "engine/daily_bar_repair_selftest.py", "subprocess.run(base + [\"--recover-child\", str(parent)], capture_output=True, text=True, timeout=40)", "DAILY_REPAIR_CONFINED_AND_FIXED_CHILDREN"),
    ("engine.addstock_watchdog_selftest:test_parallel_engine_boundaries.run_case.fake_gap:adapter_fetch:1",
     "engine/addstock_watchdog_selftest.py", "adapter.fetch()",
     "WATCHDOG_FAKE_ADAPTER"),
    ("engine.addstock_watchdog_selftest:test_recovery_teardown_race.fake_gap:adapter_fetch:1",
     "engine/addstock_watchdog_selftest.py", "adapter.fetch()",
     "WATCHDOG_FAKE_ADAPTER"),
    ("engine.extended_hours_selftest:_WindowAdapter.fetch:adapter_fetch:1",
     "engine/extended_hours_selftest.py", "super().fetch(contract, end_dt, duration, bar_size)",
     "WINDOW_FAKE_ADAPTER"),
    ("engine.external_sweep_selftest:gate_tests:ibkr_non_req:1",
     "engine/external_sweep_selftest.py", "adapter.connect()",
     "SWEEP_FAKE_OR_PARSER"),
    ("engine.external_sweep_selftest:provider_tests:adapter_fetch:1",
     "engine/external_sweep_selftest.py", "provider.fetch(\"TEST\")",
     "SWEEP_FAKE_OR_PARSER"),
    ("engine.external_sweep_selftest:provider_tests:adapter_fetch:2",
     "engine/external_sweep_selftest.py", "provider.fetch(\"TEST\")",
     "SWEEP_FAKE_OR_PARSER"),
    ("engine.fetch_a2_http_workflows_selftest:HttpLabels.a55_opener_scheme_witness:http_opener_open:1",
     "engine/fetch_a2_http_workflows_selftest.py", "urllib.request.OpenerDirector.open(opener, wire, timeout=deadline.remaining())",
     "HTTP_CONNECT_TRIPWIRE"),
    ("engine.fetch_a2_http_workflows_selftest:HttpLabels.a56_error_processing_witness:http_opener_open:1",
     "engine/fetch_a2_http_workflows_selftest.py", "urllib.request.OpenerDirector.open(opener, wire, timeout=deadline.remaining())",
     "HTTP_CONNECT_TRIPWIRE"),
    ("engine.fetch_a2_http_workflows_selftest:HttpLabels.test_a53_parent_connect_tripwire_and_socket_shutdown_are_retained:ibkr_non_req:1",
     "engine/fetch_a2_http_workflows_selftest.py", "connection.connect()",
     "HTTP_CONNECT_TRIPWIRE"),
    ("engine.fetch_a2_http_workflows_selftest:HttpLabels.a64_transport_guard_witness:ibkr_non_req:1",
     "engine/fetch_a2_http_workflows_selftest.py", "connection.connect()",
     "HTTP_CONNECT_TRIPWIRE"),
    ("engine.fetch_a2_http_workflows_selftest:HttpLabels.a64_transport_guard_witness:ibkr_non_req:2",
     "engine/fetch_a2_http_workflows_selftest.py", "connection.connect()",
     "HTTP_CONNECT_TRIPWIRE"),
    ("engine.fetch_a2_http_workflows_selftest:HttpLabels.test_a60_tls_cleanup_preserves_primary_failure:ibkr_non_req:1",
     "engine/fetch_a2_http_workflows_selftest.py", "connection.connect()",
     "HTTP_CONNECT_TRIPWIRE"),
    ("engine.fetch_a2_ibkr_workflows_selftest:Operations.scheduler_driver_fixture.SchedulerRaw.reqHistoricalDataAsync:adapter_fetch:1",
     "engine/fetch_a2_ibkr_workflows_selftest.py", "raw.source.fetch(pinned, end, kwargs[\"durationStr\"], kwargs[\"barSizeSetting\"])",
     "OPERATIONS_CONFINED"),
    ("engine.fetch_a2_ibkr_workflows_selftest:Operations.session_property_adapter.witness:adapter_fetch:1",
     "engine/fetch_a2_ibkr_workflows_selftest.py", "thief.fetch(contract(), datetime(2026, 7, 31, 23, 59), \"2 M\", \"1 day\")",
     "OPERATIONS_CONFINED"),
    ("engine.fetch_a2_ibkr_workflows_selftest:Operations.session_property_adapter.witness:adapter_head:1",
     "engine/fetch_a2_ibkr_workflows_selftest.py", "thief.head_timestamp(contract())",
     "OPERATIONS_CONFINED"),
    ("engine.fetch_a2_ibkr_workflows_selftest:Operations.test_shared_session_properties_restore_scope_and_state_on_failure:adapter_fetch:1",
     "engine/fetch_a2_ibkr_workflows_selftest.py", "adapter.fetch(contract(), datetime(2026, 7, 31, 23, 59), \"2 M\", \"1 day\")",
     "OPERATIONS_CONFINED"),
    ("engine.fetch_a2_ibkr_workflows_selftest:Operations.test_head_session_property_is_observer_before_request_consumption:adapter_head:1",
     "engine/fetch_a2_ibkr_workflows_selftest.py", "adapter.head_timestamp(contract())",
     "OPERATIONS_CONFINED"),
    ("engine.fetch_a2_ibkr_workflows_selftest:Operations.injected_composition_controls:socket:1",
     "engine/fetch_a2_ibkr_workflows_selftest.py", "socket.socket()",
     "OPERATIONS_CONFINED"),
    ("engine.fetch_a2_ibkr_workflows_selftest:Operations.test_retired_provider_exception_hooks_are_observers.witness:adapter_fetch:1",
     "engine/fetch_a2_ibkr_workflows_selftest.py", "thief.fetch(contract(), datetime(2026, 7, 1, 16), \"1 D\", \"1 day\")",
     "OPERATIONS_CONFINED"),
    ("engine.fetch_a2_ibkr_workflows_selftest:Operations.test_retired_provider_exception_hooks_are_observers.witness:adapter_head:1",
     "engine/fetch_a2_ibkr_workflows_selftest.py", "thief.head_timestamp(contract())",
     "OPERATIONS_CONFINED"),
    ("engine.fetch_a2_ibkr_workflows_selftest:Operations.test_probe_reuse_requires_published_sealed_source.observe:adapter_fetch:1",
     "engine/fetch_a2_ibkr_workflows_selftest.py", "ib.LiveIB.fetch(thief, contract(), datetime(2026, 7, 1, 16), \"1 D\", \"1 day\")",
     "OPERATIONS_CONFINED"),
    ("engine.fetch_a2_ibkr_workflows_selftest:Operations.test_probe_reuse_requires_published_sealed_source.observe:adapter_head:1",
     "engine/fetch_a2_ibkr_workflows_selftest.py", "thief.head_timestamp(contract())",
     "OPERATIONS_CONFINED"),
    ("engine.fetch_a2_ibkr_workflows_selftest:Operations.test_probe_adapter_descriptors_are_observers.descriptor:adapter_fetch:1",
     "engine/fetch_a2_ibkr_workflows_selftest.py", "ib.LiveIB.fetch(thief, contract(), datetime(2026, 7, 1, 16), \"1 D\", \"1 day\")",
     "OPERATIONS_CONFINED"),
    ("engine.fetch_a2_ibkr_workflows_selftest:Operations.test_probe_adapter_descriptors_are_observers.descriptor:adapter_head:1",
     "engine/fetch_a2_ibkr_workflows_selftest.py", "thief.head_timestamp(contract())",
     "OPERATIONS_CONFINED"),
    ("engine.fetch_a2_ibkr_workflows_selftest:Operations.test_probe_exception_hooks_are_observers_and_preserve_failure.witness:adapter_fetch:1",
     "engine/fetch_a2_ibkr_workflows_selftest.py", "adapter.fetch(contract(), datetime(2026, 7, 1, 16), \"1 D\", \"1 day\")",
     "OPERATIONS_CONFINED"),
    ("engine.fetch_a2_ibkr_workflows_selftest:Operations.test_probe_exception_hooks_are_observers_and_preserve_failure.witness:adapter_head:1",
     "engine/fetch_a2_ibkr_workflows_selftest.py", "adapter.head_timestamp(contract())",
     "OPERATIONS_CONFINED"),
    ("engine.fetch_a2_ibkr_workflows_selftest:Operations.c2_observer_attack_fixture.witness:adapter_fetch:1",
     "engine/fetch_a2_ibkr_workflows_selftest.py", "adapter.fetch(contract(), datetime(2026, 7, 1, 16), \"1 D\", \"1 min\", ib._what_to_show(token))",
     "OPERATIONS_CONFINED"),
    ("engine.fetch_a2_ibkr_workflows_selftest:Operations.fixdata_multiport_recovery_identity.Transport.qualifyContractsAsync:ibkr_non_req:1",
     "engine/fetch_a2_ibkr_workflows_selftest.py", "super().qualifyContractsAsync(pinned)",
     "OPERATIONS_CONFINED"),
    ("engine.fetch_a2_ibkr_workflows_selftest:Operations.fixdata_multiport_recovery_identity.Transport.reqHeadTimeStampAsync:ibkr_raw_head:1",
     "engine/fetch_a2_ibkr_workflows_selftest.py", "super().reqHeadTimeStampAsync(pinned, **kwargs)",
     "OPERATIONS_CONFINED"),
    ("engine.fetch_a2_ibkr_workflows_selftest:Operations.fixdata_multiport_recovery_identity.Transport.reqHistoricalDataAsync:ibkr_raw_bars:1",
     "engine/fetch_a2_ibkr_workflows_selftest.py", "super().reqHistoricalDataAsync(pinned, **kwargs)",
     "OPERATIONS_CONFINED"),
    ("engine.fetch_a2_ibkr_workflows_selftest:Operations.test_c2_fixdata_callback_families_and_removal_inverses.witness:adapter_fetch:1",
     "engine/fetch_a2_ibkr_workflows_selftest.py", "attack_adapter.fetch(pinned, end, \"1 D\", \"1 min\", \"TRADES\")",
     "OPERATIONS_CONFINED"),
    ("engine.fetch_a2_ibkr_workflows_selftest:Operations.test_c2_addstock_root_observer_authority_and_removal_inverse.witness:adapter_fetch:1",
     "engine/fetch_a2_ibkr_workflows_selftest.py", "attack_adapter.fetch(pinned, end, \"1 D\", \"1 min\", \"TRADES\")",
     "OPERATIONS_CONFINED"),
    ("engine.fetch_a2_ibkr_workflows_selftest:Operations.test_d2_internal_observers_cannot_borrow.observer:adapter_fetch:1",
     "engine/fetch_a2_ibkr_workflows_selftest.py", "adapter.fetch(contract(), self.fixture_now, \"1 D\", \"1 min\")",
     "OPERATIONS_CONFINED"),
    ("engine.fetch_a2_ibkr_workflows_selftest:Operations.test_d06_older_head_thirty_year_floor_and_expanded_max_send.max_body:adapter_fetch:1",
     "engine/fetch_a2_ibkr_workflows_selftest.py", "adapter.fetch(contract(), raw_end, \"60 Y\", \"1 day\")",
     "OPERATIONS_CONFINED"),
    ("engine.fetch_a2_ibkr_workflows_selftest:Operations.test_daily_send_time_unsettled_refusal_has_independent_removal_pin.body:adapter_fetch:1",
     "engine/fetch_a2_ibkr_workflows_selftest.py", "adapter.fetch(contract(), end, \"1 D\", \"1 day\", what_to_show=bridge.KINDS[bridge.parse_token(token)[1]])",
     "OPERATIONS_CONFINED"),
    ("engine.fetch_a2_infrastructure_selftest:Infrastructure.test_transport_tripwire_is_real:socket_dns:1",
     "engine/fetch_a2_infrastructure_selftest.py", "socket.getaddrinfo(\"example.invalid\", 443)",
     "DNS_REFUSAL_PROBE"),
    ("engine.fetch_ibkr_integration_selftest:Integration.test_missing_context_every_live_choke:adapter_fetch:1",
     "engine/fetch_ibkr_integration_selftest.py", "self.adapter.fetch(contract(), datetime(2026, 7, 8, 16), \"1 D\", \"1 day\")",
     "IBKR_CONFINED_CHOKE"),
    ("engine.fetch_ibkr_integration_selftest:Integration.test_missing_context_every_live_choke:adapter_head:1",
     "engine/fetch_ibkr_integration_selftest.py", "self.adapter.head_timestamp(contract())",
     "IBKR_CONFINED_CHOKE"),
    ("engine.fetch_ibkr_integration_selftest:Integration.test_scope_consumed_before_observer_can_reenter.acquire:adapter_fetch:1",
     "engine/fetch_ibkr_integration_selftest.py", "self.adapter.fetch(contract(), datetime(2026, 7, 8, 16), \"1 D\", \"1 day\")",
     "IBKR_CONFINED_CHOKE"),
    ("engine.fetch_ibkr_integration_selftest:Integration.test_inverse_choke_guard_bypass_reaches_raw_send:adapter_fetch:1",
     "engine/fetch_ibkr_integration_selftest.py", "self.adapter.fetch(contract(), datetime(2026, 7, 8, 16), \"1 D\", \"1 day\")",
     "IBKR_CONFINED_CHOKE"),
    ("engine.fetch_ibkr_integration_selftest:Integration.test_variant_only_mismatch_at_choke.call:adapter_head:1",
     "engine/fetch_ibkr_integration_selftest.py", "self.adapter.head_timestamp(contract())",
     "IBKR_CONFINED_CHOKE"),
    ("engine.fetch_ibkr_integration_selftest:Integration.test_no_data_does_not_mask_permission_contract_pacing_or_unknown.call:adapter_fetch:1",
     "engine/fetch_ibkr_integration_selftest.py", "self.adapter.fetch(contract(), datetime(2026, 7, 8, 16), \"1 D\", \"1 day\")",
     "IBKR_CONFINED_CHOKE"),
    ("engine.fetch_pickup_selftest:deep_head_floor_is_not_cache_authority.DeepHeadAdapter.fetch:adapter_fetch:1",
     "engine/fetch_pickup_selftest.py", "super().fetch( contract, end_dt, duration, bar_size, what_to_show)",
     "PICKUP_FAKE_ADAPTER"),
    ("engine.fetch_pickup_selftest:parallel_shared_pickup.RacingAdapter.head_timestamp:adapter_head:1",
     "engine/fetch_pickup_selftest.py", "super().head_timestamp(contract, what_to_show)",
     "PICKUP_FAKE_ADAPTER"),
    ("engine.fetch_pickup_selftest:parallel_shared_pickup.HeadOnlyParallelAdapter.fetch:adapter_fetch:1",
     "engine/fetch_pickup_selftest.py", "super().fetch( contract, end_dt, duration, bar_size, what_to_show)",
     "PICKUP_FAKE_ADAPTER"),
    ("engine.fetch_pickup_selftest:parallel_shared_pickup.SidecarRaceAdapter.head_timestamp:adapter_head:1",
     "engine/fetch_pickup_selftest.py", "super().head_timestamp(contract, what_to_show)",
     "PICKUP_FAKE_ADAPTER"),
    ("engine.fetch_pickup_selftest:pacing_wait_status.StatusAdapter.fetch:adapter_fetch:1",
     "engine/fetch_pickup_selftest.py", "super().fetch(contract, end_dt, duration, bar_size, what_to_show)",
     "PICKUP_FAKE_ADAPTER"),
    ("engine.split_provider_reference:_fetch_bytes:http_urlopen:1",
     "engine/split_provider_reference.py", "urllib.request.urlopen(request, timeout=timeout)",
     "PHASE0_MUST_BE_HELD"),
    ("engine.stock_ibkr_selftest:DeadPinnedAdapter.fetch:adapter_fetch:1",
     "engine/stock_ibkr_selftest.py", "super().fetch(contract, end_dt, duration, bar_size, what_to_show)",
     "STOCK_FAKE_AND_LOOPBACK"),
    ("engine.stock_ibkr_selftest:NonContractPinnedAdapter.fetch:adapter_fetch:1",
     "engine/stock_ibkr_selftest.py", "FakeAdapter.fetch( self, contract, end_dt, duration, bar_size, what_to_show)",
     "STOCK_FAKE_AND_LOOPBACK"),
    ("engine.stock_ibkr_selftest:PartialOldPinnedAdapter.fetch:adapter_fetch:1",
     "engine/stock_ibkr_selftest.py", "FakeAdapter.fetch( self, contract, end_dt, duration, bar_size, what_to_show)",
     "STOCK_FAKE_AND_LOOPBACK"),
    ("engine.stock_ibkr_selftest:DieLaterAdapter.fetch:adapter_fetch:1",
     "engine/stock_ibkr_selftest.py", "super().fetch(contract, end_dt, duration, bar_size)",
     "STOCK_FAKE_AND_LOOPBACK"),
    ("engine.stock_ibkr_selftest:<module>:socket:1",
     "engine/stock_ibkr_selftest.py", "_socket.socket(_socket.AF_INET, _socket.SOCK_STREAM)",
     "STOCK_FAKE_AND_LOOPBACK"),
    ("engine.stock_ibkr_selftest:SpotMismatchAdapter.fetch:adapter_fetch:1",
     "engine/stock_ibkr_selftest.py", "super().fetch(contract, end_dt, duration, bar_size)",
     "STOCK_FAKE_AND_LOOPBACK"),
    ("engine.stock_ibkr_selftest:CancelAfterAdapter.fetch:adapter_fetch:1",
     "engine/stock_ibkr_selftest.py", "super().fetch(*a)",
     "STOCK_FAKE_AND_LOOPBACK"),
    ("engine.stock_ibkr_selftest:DieAfterAdapter.fetch:adapter_fetch:1",
     "engine/stock_ibkr_selftest.py", "super().fetch(*a)",
     "STOCK_FAKE_AND_LOOPBACK"),
    ("engine.stock_ibkr_selftest:_MidrunMaintenanceAdapter.fetch:adapter_fetch:1",
     "engine/stock_ibkr_selftest.py", "super().fetch( contract, end_dt, duration, bar_size, what_to_show=what_to_show)",
     "STOCK_FAKE_AND_LOOPBACK"),
    ("engine.stock_ibkr_selftest:_test_client_id_rotation:ibkr_non_req:1",
     "engine/stock_ibkr_selftest.py", "a.connect()",
     "STOCK_FAKE_AND_LOOPBACK"),
    ("engine.stock_ibkr_selftest:_test_client_id_rotation:ibkr_non_req:2",
     "engine/stock_ibkr_selftest.py", "a2.connect()",
     "STOCK_FAKE_AND_LOOPBACK"),
    ("engine.stock_ibkr_selftest:_test_client_id_rotation:ibkr_non_req:3",
     "engine/stock_ibkr_selftest.py", "b.connect()",
     "STOCK_FAKE_AND_LOOPBACK"),
    ("engine.stock_ibkr_selftest:_test_client_id_rotation:ibkr_non_req:4",
     "engine/stock_ibkr_selftest.py", "c.connect()",
     "STOCK_FAKE_AND_LOOPBACK"),
    ("engine.stock_ibkr_selftest:_test_earliest_available_and_sidecar.IdentityFake.fetch:adapter_fetch:1",
     "engine/stock_ibkr_selftest.py", "super().fetch(contract, end, dur, bar, what_to_show)",
     "STOCK_FAKE_AND_LOOPBACK"),
    ("engine.tws_discovery_reference:f4_port_scan:socket:1",
     "engine/tws_discovery_reference.py", "socket.socket()",
     "DISCOVERY_LOOPBACK"),
    ("engine.tws_discovery_reference:f4_port_scan:socket:2",
     "engine/tws_discovery_reference.py", "socket.socket()",
     "DISCOVERY_LOOPBACK"),
)

_EXCLUDED_SEND_DISPOSITIONS += (
    ("engine.check_kit_selftest:test_subprocess_exit_codes:census_process_launch:1", "engine/check_kit_selftest.py", "subprocess.run( [sys.executable, \"-c\", template.format( body=body, absent=absent)], cwd=PROJECT_ROOT, capture_output=True, text=True, timeout=20)", "LOCAL_HARNESS_CHILD"),
    ("engine.dependency_manifest_selftest:absence_contract:census_process_launch:1", "engine/dependency_manifest_selftest.py", "subprocess.run( [sys.executable, \"-B\", \"-c\", _ABSENCE_CODE, row.import_name], cwd=ROOT, capture_output=True, text=True, timeout=40, env={**os.environ, \"PYTHONDONTWRITEBYTECODE\": \"1\"})", "LOCAL_HARNESS_CHILD"),
    ("engine.dependency_manifest_selftest:copied_contract:census_process_launch:1", "engine/dependency_manifest_selftest.py", "subprocess.run( [sys.executable, \"-B\", \"-c\", script], cwd=copy, capture_output=True, text=True, timeout=60, env={**os.environ, \"PYTHONDONTWRITEBYTECODE\": \"1\", \"PYTHONPATH\": str(copy)})", "LOCAL_HARNESS_CHILD"),
    ("engine.export_source_line_reference:mutation_main.run:census_process_launch:1", "engine/export_source_line_reference.py", "subprocess.run( [sys.executable, str(path)], cwd=str(path.parent.parent), env=clean_env, text=True, capture_output=True, timeout=120, encoding=\"utf-8\", errors=\"replace\")", "LOCAL_HARNESS_CHILD"),
    ("engine.external_sweep_selftest:gate_tests:census_process_launch:1", "engine/external_sweep_selftest.py", "subprocess.Popen( [sys.executable, \"-c\", code, str(child_lock), str(engine_dir)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)", "SWEEP_FAKE_OR_PARSER"),
    ("engine.fetch_a2_ibkr_workflows_selftest:Operations.test_c2_legacy_ratio_driver_boundaries.on_fetch:census_process_launch:1", "engine/fetch_a2_ibkr_workflows_selftest.py", "subprocess.run([\"must-not-launch\"])", "OPERATIONS_CONFINED"),
    ("engine.fetch_horizon_selftest:<module>:census_process_launch:1", "engine/fetch_horizon_selftest.py", ("sha256", "481bac0909e6a46c14cba32880a13c07d22c5d479d3ddd1e889f533f1f5d263b"), "LOCAL_HARNESS_CHILD"),
    ("engine.midnight_task_installer_selftest:main:census_process_launch:1", "engine/midnight_task_installer_selftest.py", "subprocess.run( [sys.executable, \"-B\", str(copied_installer), \"--dry-run\", \"--json\"], cwd=temp_root, capture_output=True, text=True, encoding=\"utf-8\", errors=\"replace\", timeout=30, check=False, )", "LOCAL_HARNESS_CHILD"),
    ("engine.ordinary_correction_write_selftest:hard_exit_matrix:census_process_launch:1", "engine/ordinary_correction_write_selftest.py", "subprocess.run( [sys.executable, \"-B\", script, \"--child\", writer, str(root), ticker, phase], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=30)", "LOCAL_HARNESS_CHILD"),
    ("engine.portability_reference:main:census_process_launch:1", "engine/portability_reference.py", "subprocess.run([sys.executable, str(probe)], capture_output=True, text=True, timeout=60)", "LOCAL_HARNESS_CHILD"),
    ("engine.portability_reference:main:census_process_launch:2", "engine/portability_reference.py", "subprocess.run([sys.executable, str(imp)], capture_output=True, text=True, timeout=120)", "LOCAL_HARNESS_CHILD"),
    ("engine.run_gates:_terminate_process_tree:census_process_launch:1", "engine/run_gates.py", "subprocess.run( [\"taskkill\", \"/PID\", str(pid), \"/T\", \"/F\"], stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False, timeout=10, )", "LOCAL_HARNESS_CHILD"),
    ("engine.run_gates:_execute_suite:census_process_launch:1", "engine/run_gates.py", "subprocess.Popen( [sys.executable, \"-B\", str(script)], cwd=scratch, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=environment, bufsize=0, **popen_options, )", "LOCAL_HARNESS_CHILD"),
    ("engine.run_gates_selftest:invoke_stub_process:census_process_launch:1", "engine/run_gates_selftest.py", "subprocess.run( [sys.executable, \"-B\", \"-c\", code], cwd=PROJECT_ROOT, env=child_env, capture_output=True, text=True, encoding=\"utf-8\", errors=\"replace\", timeout=20, check=False, )", "LOCAL_HARNESS_CHILD"),
    ("engine.run_gates_selftest:test_surface_and_default_registry:census_process_launch:1", "engine/run_gates_selftest.py", "subprocess.run( [sys.executable, \"-B\", str(ENGINE_ROOT / \"run_gates.py\"), \"--list\"], cwd=Path(tempfile.gettempdir()), capture_output=True, text=True, encoding=\"utf-8\", errors=\"replace\", timeout=10, check=False)", "LOCAL_HARNESS_CHILD"),
    ("engine.run_gates_selftest:test_preflight_and_cli_rejection:census_process_launch:1", "engine/run_gates_selftest.py", "subprocess.run( [sys.executable, \"-B\", str(ENGINE_ROOT / \"run_gates.py\"), \"export\", \"--repeat\", \"0\"], cwd=PROJECT_ROOT, capture_output=True, text=True, encoding=\"utf-8\", errors=\"replace\", timeout=10, check=False)", "LOCAL_HARNESS_CHILD"),
    ("engine.run_gates_selftest:test_preflight_and_cli_rejection:census_process_launch:2", "engine/run_gates_selftest.py", "subprocess.run( [sys.executable, \"-B\", str(ENGINE_ROOT / \"run_gates.py\"), \"unknown\"], cwd=PROJECT_ROOT, capture_output=True, text=True, encoding=\"utf-8\", errors=\"replace\", timeout=10, check=False)", "LOCAL_HARNESS_CHILD"),
    ("engine.run_gates_selftest:test_preflight_and_cli_rejection:census_process_launch:3", "engine/run_gates_selftest.py", "subprocess.run( [sys.executable, \"-B\", str(ENGINE_ROOT / \"run_gates.py\"), \"--script\", \"anything.py\"], cwd=PROJECT_ROOT, capture_output=True, text=True, encoding=\"utf-8\", errors=\"replace\", timeout=10, check=False)", "LOCAL_HARNESS_CHILD"),
    ("engine.run_gates_selftest:test_preflight_and_cli_rejection:census_process_launch:4", "engine/run_gates_selftest.py", "subprocess.run( [sys.executable, \"-B\", str(ENGINE_ROOT / \"run_gates.py\"), \"--list\", \"--repeat\", \"2\"], cwd=PROJECT_ROOT, capture_output=True, text=True, encoding=\"utf-8\", errors=\"replace\", timeout=10, check=False)", "LOCAL_HARNESS_CHILD"),
    ("engine.run_gates_selftest:test_stock_ibkr_regression_mutation:census_process_launch:1", "engine/run_gates_selftest.py", "subprocess.run( [sys.executable, \"-B\", str(copied_engine / \"run_gates.py\"), \"stock-ibkr\"], cwd=root, capture_output=True, text=True, encoding=\"utf-8\", errors=\"replace\", timeout=180, check=False)", "LOCAL_HARNESS_CHILD"),
    ("engine.testbank_selftest:test_unsafe_root_containment:census_process_launch:1", "engine/testbank_selftest.py", "subprocess.run( [\"cmd\", \"/c\", \"mklink\", \"/J\", str(alias), str(production)], capture_output=True, text=True, check=False, timeout=10, )", "LOCAL_HARNESS_CHILD"),
    ("engine.testbank_selftest:test_foreign_gate_identity_fails_closed:census_process_launch:1", "engine/testbank_selftest.py", "subprocess.run( [sys.executable, \"-c\", script, str(PROJECT_ROOT)], cwd=PROJECT_ROOT, capture_output=True, text=True, check=False, timeout=10, )", "LOCAL_HARNESS_CHILD"),
    ("engine.tier0_mcp_selftest:process_tests:census_process_launch:1", "engine/tier0_mcp_selftest.py", "subprocess.Popen( _child_command(fixture), stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)", "LOCAL_HARNESS_CHILD"),
    ("engine.vol_value_audit_selftest:main:census_process_launch:1", "engine/vol_value_audit_selftest.py", "subprocess.Popen( [sys.executable, \"-c\", child_code, engine_dir, str(process_root), f\"P{index}\"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)", "LOCAL_HARNESS_CHILD"),
    ("engine.vol_value_reconcile_selftest:run_hard_exit_recovery_checks:census_process_launch:1", "engine/vol_value_reconcile_selftest.py", "subprocess.run( [sys.executable, \"-B\", str(Path(__file__).resolve()), \"--crash-worker\", str(root), phase], cwd=str(Path(__file__).resolve().parent.parent), capture_output=True, text=True, timeout=30, check=False)", "LOCAL_HARNESS_CHILD"),
    ("engine.vol_value_reconcile_selftest:run_hard_exit_recovery_checks:census_process_launch:2", "engine/vol_value_reconcile_selftest.py", "subprocess.run( [sys.executable, \"-B\", str(Path(__file__).resolve()), \"--crash-worker\", str(public_root), \"month\"], cwd=str(Path(__file__).resolve().parent.parent), capture_output=True, text=True, timeout=30, check=False)", "LOCAL_HARNESS_CHILD"),
    ("engine.vol_value_reconcile_selftest:run_hard_exit_recovery_checks:census_process_launch:3", "engine/vol_value_reconcile_selftest.py", "subprocess.run( [sys.executable, \"-B\", str(Path(__file__).resolve()), \"--crash-worker\", str(path_root), \"month\"], cwd=str(Path(__file__).resolve().parent.parent), capture_output=True, text=True, timeout=30, check=False)", "LOCAL_HARNESS_CHILD"),
)
_EXCLUDED_PROOF_SOURCES = {
    "engine/daily_bar_repair_selftest.py": ("fb181eac3b8446744528347db07998be2f21dc4cb8fd01fc3d205a523a800296",
        "DAILY_REPAIR_CONFINED_AND_FIXED_CHILDREN", ("with policy.offline_transports():", "self.guard = policy.offline_transports()", "self.raw = self.adapter.ib = RawIB([bar(DAY)])", "base = [sys.executable, \"-B\", str(Path(__file__).resolve())]", "supplied.parent != root.resolve()", "b\"A182 synthetic crash fixture\\n\"", "with policy.offline_transports(), patch.object(repair.operation_gate, \"acquire\",",)),
    "engine/check_kit_selftest.py": ("93f1efe24eb16cf2e887cf666909f94c99904b75f9ab53e2b684e84ea30a49be", "LOCAL_HARNESS_CHILD", ("subprocess.run(\n            [sys.executable, \"-c\", template.format(\n                body=body, absent=absent)],\n            cwd=PROJECT_ROOT, capture_output=True, text=True, timeout=20)",)),
    "engine/dependency_manifest_selftest.py": ("08b5df2dae75f788ffe9b22dcd075f6e9f7af5bf6a2922109de21121d61c93b9", "LOCAL_HARNESS_CHILD", ("subprocess.run(\n            [sys.executable, \"-B\", \"-c\", _ABSENCE_CODE, row.import_name],\n            cwd=ROOT, capture_output=True, text=True, timeout=40,\n            env={**os.environ, \"PYTHONDONTWRITEBYTECODE\": \"1\"})", "subprocess.run(\n            [sys.executable, \"-B\", \"-c\", script], cwd=copy,\n            capture_output=True, text=True, timeout=60,\n            env={**os.environ, \"PYTHONDONTWRITEBYTECODE\": \"1\", \"PYTHONPATH\": str(copy)})",)),
    "engine/export_source_line_reference.py": ("98919ecb3dfbb5a8c515b5951e8a943be974d1c196d0524061f7abfa4334dfbe", "LOCAL_HARNESS_CHILD", ("subprocess.run(\n            [sys.executable, str(path)], cwd=str(path.parent.parent),\n            env=clean_env, text=True, capture_output=True, timeout=120,\n            encoding=\"utf-8\", errors=\"replace\")",)),
    "engine/fetch_horizon_selftest.py": ("85bb549a25d2872d0553186bb0a0e723557d9d04fda8eac2ee99bea737012afe", "LOCAL_HARNESS_CHILD", ("cwd=relocated, capture_output=True, text=True, timeout=30)",)),
    "engine/midnight_task_installer_selftest.py": ("934bcc325e1c58dfc789badddf958ce8fe126b7aa12c4105458d18caee998469", "LOCAL_HARNESS_CHILD", ("subprocess.run(\n            [sys.executable, \"-B\", str(copied_installer),\n             \"--dry-run\", \"--json\"],\n            cwd=temp_root,\n            capture_output=True,\n            text=True,\n            encoding=\"utf-8\",\n            errors=\"replace\",\n            timeout=30,\n            check=False,\n        )",)),
    "engine/ordinary_correction_write_selftest.py": ("c1e7bc611b9b3e8904846a0e82aa7daf5973335a8fb254848ec4b3acd408c42d", "LOCAL_HARNESS_CHILD", ("subprocess.run(\n                [sys.executable, \"-B\", script, \"--child\", writer,\n                 str(root), ticker, phase],\n                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,\n                timeout=30)",)),
    "engine/portability_reference.py": ("1d10f122bb23c696fa1e8bb5c1d01aec949674fa68b1f357e35739f0251d0755", "LOCAL_HARNESS_CHILD", ("subprocess.run([sys.executable, str(probe)],\n                             capture_output=True, text=True, timeout=60)", "subprocess.run([sys.executable, str(imp)],\n                              capture_output=True, text=True, timeout=120)", "str(probe)", "str(imp)",)),
    "engine/run_gates.py": ("794fd6b1a385a4227ad09ad99d0fa71ade3cf98b5416e1eca294aeec21e3369e", "LOCAL_HARNESS_CHILD", ("subprocess.run(\n                    [\"taskkill\", \"/PID\", str(pid), \"/T\", \"/F\"],\n                    stdin=subprocess.DEVNULL,\n                    stdout=subprocess.DEVNULL,\n                    stderr=subprocess.DEVNULL,\n                    check=False,\n                    timeout=10,\n                )", "subprocess.Popen(\n                    [sys.executable, \"-B\", str(script)],\n                    cwd=scratch,\n                    stdin=subprocess.DEVNULL,\n                    stdout=subprocess.PIPE,\n                    stderr=subprocess.STDOUT,\n                    env=environment,\n                    bufsize=0,\n                    **popen_options,\n                )",)),
    "engine/run_gates_selftest.py": ("07d6c8a81e093f83e3d504ec4df8e0d0577bda346e5d031e496cdf97d3711a78", "LOCAL_HARNESS_CHILD", ("subprocess.run(\n        [sys.executable, \"-B\", \"-c\", code],\n        cwd=PROJECT_ROOT,\n        env=child_env,\n        capture_output=True,\n        text=True,\n        encoding=\"utf-8\",\n        errors=\"replace\",\n        timeout=20,\n        check=False,\n    )", "subprocess.run(\n        [sys.executable, \"-B\", str(ENGINE_ROOT / \"run_gates.py\"), \"--list\"],\n        cwd=Path(tempfile.gettempdir()), capture_output=True, text=True,\n        encoding=\"utf-8\", errors=\"replace\", timeout=10, check=False)", "subprocess.run(\n        [sys.executable, \"-B\", str(ENGINE_ROOT / \"run_gates.py\"),\n         \"export\", \"--repeat\", \"0\"],\n        cwd=PROJECT_ROOT, capture_output=True, text=True,\n        encoding=\"utf-8\", errors=\"replace\", timeout=10, check=False)", "subprocess.run(\n        [sys.executable, \"-B\", str(ENGINE_ROOT / \"run_gates.py\"), \"unknown\"],\n        cwd=PROJECT_ROOT, capture_output=True, text=True,\n        encoding=\"utf-8\", errors=\"replace\", timeout=10, check=False)", "subprocess.run(\n        [sys.executable, \"-B\", str(ENGINE_ROOT / \"run_gates.py\"),\n         \"--script\", \"anything.py\"],\n        cwd=PROJECT_ROOT, capture_output=True, text=True,\n        encoding=\"utf-8\", errors=\"replace\", timeout=10, check=False)", "subprocess.run(\n        [sys.executable, \"-B\", str(ENGINE_ROOT / \"run_gates.py\"),\n         \"--list\", \"--repeat\", \"2\"],\n        cwd=PROJECT_ROOT, capture_output=True, text=True,\n        encoding=\"utf-8\", errors=\"replace\", timeout=10, check=False)", "subprocess.run(\n            [sys.executable, \"-B\", str(copied_engine / \"run_gates.py\"),\n             \"stock-ibkr\"],\n            cwd=root, capture_output=True, text=True,\n            encoding=\"utf-8\", errors=\"replace\", timeout=180, check=False)",)),
    "engine/testbank_selftest.py": ("b7e7406ee27401b0d86f193f0a24137e41db14cd75ccaefdb20d40d911dcb0a7", "LOCAL_HARNESS_CHILD", ("subprocess.run(\n        [sys.executable, \"-c\", script, str(PROJECT_ROOT)],\n        cwd=PROJECT_ROOT,\n        capture_output=True,\n        text=True,\n        check=False,\n        timeout=10,\n    )", "subprocess.run(\n                    [\"cmd\", \"/c\", \"mklink\", \"/J\", str(alias),\n                     str(production)],\n                    capture_output=True,\n                    text=True,\n                    check=False,\n                    timeout=10,\n                )",)),
    "engine/tier0_mcp_selftest.py": ("fe59994d1bab494119b611da0a876bfe6b920cc3b882962a1a252faafd7cf323", "LOCAL_HARNESS_CHILD", ("subprocess.Popen(\n            _child_command(fixture), stdin=subprocess.PIPE,\n            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)",)),
    "engine/vol_value_audit_selftest.py": ("01a584a1ee22787450857934216b325da59d92437283fb77bd5575bde66451df", "LOCAL_HARNESS_CHILD", ("subprocess.Popen(\n                [sys.executable, \"-c\", child_code, engine_dir,\n                 str(process_root), f\"P{index}\"],\n                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)",)),
    "engine/vol_value_reconcile_selftest.py": ("57f8f871ca9917796875fd8ddb05070b1374561175d59b4359a5814a47266e97", "LOCAL_HARNESS_CHILD", ("subprocess.run(\n        [sys.executable, \"-B\", str(Path(__file__).resolve()),\n         \"--crash-worker\", str(public_root), \"month\"],\n        cwd=str(Path(__file__).resolve().parent.parent),\n        capture_output=True, text=True, timeout=30, check=False)", "subprocess.run(\n        [sys.executable, \"-B\", str(Path(__file__).resolve()),\n         \"--crash-worker\", str(path_root), \"month\"],\n        cwd=str(Path(__file__).resolve().parent.parent),\n        capture_output=True, text=True, timeout=30, check=False)", "subprocess.run(\n            [sys.executable, \"-B\", str(Path(__file__).resolve()),\n             \"--crash-worker\", str(root), phase],\n            cwd=str(Path(__file__).resolve().parent.parent),\n            capture_output=True, text=True, timeout=30, check=False)",)),
    "engine/addstock_watchdog_selftest.py": ("a85394217eb0e597db76e9da9af3ee9c1b406bb36364346629d46eef4c5d5087",
        "WATCHDOG_FAKE_ADAPTER", ("class Adapter:","return lambda: Adapter(int(ports[0]))","def fake_gap(",)),
    "engine/extended_hours_selftest.py": ("755a00250fb4328b5a8578dab9b02e8f2cf79f074ac98d5501985ebe3043e51b",
        "WINDOW_FAKE_ADAPTER", ("class ExtAdapter:","class _WindowAdapter(ExtAdapter):",)),
    "engine/external_sweep_selftest.py": ("7c3078403442464daf0dc292325ad32880492e06a8fa52720d2862d4193ea3f5",
        "SWEEP_FAKE_OR_PARSER", ("stock_ibkr.LiveIB._connect_under_gate = lambda self: self","opener=opener, now=FIXED_NOW, timeout=7","with patch(\"fetch_ibkr_bridge.refuse_a2\"):",)),
    "engine/fetch_a2_http_workflows_selftest.py": ("fd36f1ad8bc2fc87ba265151783b1dcf056a96acdbe7a26b094eb372aebdb67a",
        "HTTP_CONNECT_TRIPWIRE", ("with tp.offline_transports():","self.guard = tp.offline_transports()","patch.object(http_transport.http.client.HTTPSConnection, 'connect')",
        "def a55_opener_scheme_witness(self, endpoint, scheme):",
        "patch.object(http_transport.DeadlineHTTPSConnection, 'connect', truncated_connect)",
        "patch.object(http_transport, '_lookup_addresses', lookup)",
        "def protocol_tripwire(handler, request):",
        "def a56_error_processing_witness(self, endpoint, code):",
        "patch.object(http_transport.DeadlineHTTPSHandler, 'https_open', one_wire)",)),
    "engine/fetch_a2_ibkr_workflows_selftest.py": ("9ea680eac4c2192e9a02980ea1b287c25bffed6ebaa1ca7ed2eff0d600ee1644",
        "OPERATIONS_CONFINED", ("with tp.offline_transports():","self.guard = tp.offline_transports()","from fetch_ibkr_integration_selftest import RawIB, bar, contract, InstantPacer",)),
    "engine/fetch_a2_infrastructure_selftest.py": ("17131d001f5dec8d2865b2aeb12223d7fa6691580aba89368d896f32f29fad32",
        "DNS_REFUSAL_PROBE", ("with tp.offline_transports():","self.guard = tp.offline_transports()","with self.assertRaisesRegex(AuthorityError, \"tripwire\"):",)),
    "engine/fetch_ibkr_integration_selftest.py": ("db09992d55c0d8e0e9872f2d0cd7838b09bb7f23c0addbc0e88c3a63c16368d9",
        "IBKR_CONFINED_CHOKE", ("from fetch_a2_ibkr_workflows_selftest import Operations","class RawIB:","self.adapter.ib = RawIB()",)),
    "engine/fetch_pickup_selftest.py": ("267f9570b34e2a29247ebd335e1c9f6f77769d01ce56a0bc10d920b0cd27c8e0",
        "PICKUP_FAKE_ADAPTER", ("class FakeAdapter:","class DeepHeadAdapter(FakeAdapter):","class RacingAdapter(FakeAdapter):","class HeadOnlyParallelAdapter(FakeAdapter):","class SidecarRaceAdapter(FakeAdapter):","class StatusAdapter(FakeAdapter):",)),
    "engine/split_provider_reference.py": ("8ce4c950d76141188501aae2b0b79b17316353bcdefc9fcc8da27a066cbf500a",
        "PHASE0_MUST_BE_HELD", ("from fetch_ibkr_bridge import RequestRefused, refuse_a2",)),
    "engine/stock_ibkr_selftest.py": ("8eaa5edf3235ed7032af89b12a000059f0a5d2da6ca21a6946026fdb46b70611",
        "STOCK_FAKE_AND_LOOPBACK", ("class FakeAdapter:","fake.IB = _FakeIB","sys.modules[\"ib_async\"] = fake",)),
    "engine/tws_discovery_reference.py": ("de0b44e5a2b8fd9d74fe665d572f633eb561b12d9bbb065839fb99ce444a25d4",
        "DISCOVERY_LOOPBACK", ("def f4_port_scan():","s.bind((\"127.0.0.1\", 0))",)),
}
_EXCLUDED_DEPENDENCY_SHA256 = {
    "engine/fetch_test_policy.py": "89260c2281baa729282f55f34c8ded643484bd9e09478ff3f516be1e0e97f3fc",
}


def excluded_source_universe(project_root):
    paths = list(project_root.glob("*.py"))
    for directory in inv.SHIPPED_SUBDIRS:
        paths.extend((project_root / directory).rglob("*.py"))
    sources = {}
    for path in sorted(paths):
        if not path.resolve().is_relative_to(project_root.resolve()):
            raise inv.InventoryError("excluded census source leaves project: " + str(path))
        relative = path.relative_to(project_root).as_posix()
        try:
            sources[relative] = path.read_text(encoding="utf-8")
        except (OSError, UnicodeError) as exc:
            raise inv.InventoryError("cannot read excluded census source: " + relative) from exc
    return sources


@lru_cache(maxsize=256)
def _excluded_scan_cached(module, source, scanner, parser):
    # Static analysis only: exact fresh text and current function identities
    # are the cache key. No approval, effect-boundary bytes or proof is cached.
    return tuple(tuple(row.items()) for row in scanner(source, module)), parser(source)


def scan_excluded_source(source, module):
    """Census-only finite residual spellings; production scanner is unchanged.

    Recognizes literal/import aliases and simple local receiver assignments.
    It is not a proof against computed calls, arbitrary plugins or subprocess
    contents; exact source-bound dispositions still decide every candidate.
    """
    rows = inv.scan_source(source, module)
    class ResidualVisitor(inv._CandidateVisitor):
        def __init__(self):
            super().__init__(module, source)
            self.receivers = {}

        def visit_Assign(self, node):
            if isinstance(node.value, ast.Call):
                constructor = inv._resolved_dotted(node.value.func, self.aliases)
                if constructor in ('urllib.request.build_opener', 'requests.Session', 'ftplib.FTP', 'ftplib.FTP_TLS'):
                    for target in node.targets:
                        if isinstance(target, ast.Name):
                            self.receivers[target.id] = constructor
            self.generic_visit(node)

        def visit_Call(self, node):
            name = inv._resolved_dotted(node.func, self.aliases)
            primitive = None
            if name == 'urllib.request.urlretrieve':
                primitive = 'census_http_retrieve'
            elif name in ('socket.gethostbyname', 'socket.gethostbyname_ex'):
                primitive = 'census_dns_lookup'
            elif name == 'asyncio.open_connection':
                primitive = 'census_async_connect'
            elif name in ('subprocess.run', 'subprocess.Popen', 'subprocess.call',
                          'subprocess.check_call', 'subprocess.check_output',
                          'os.system', 'subprocess.getoutput', 'asyncio.create_subprocess_exec',
                          'asyncio.create_subprocess_shell', 'subprocess.getstatusoutput',
                          'os.popen', 'os.startfile',
                          'os.spawnl', 'os.spawnle', 'os.spawnlp', 'os.spawnlpe',
                          'os.spawnv', 'os.spawnve', 'os.spawnvp', 'os.spawnvpe',
                          'os.execl', 'os.execle', 'os.execlp', 'os.execlpe',
                          'os.execv', 'os.execve', 'os.execvp', 'os.execvpe',
                          'os.posix_spawn', 'os.posix_spawnp'):
                primitive = 'census_process_launch'
            elif name in ('ftplib.FTP', 'ftplib.FTP_TLS'):
                primitive = 'census_ftp_open'
            elif isinstance(node.func, ast.Attribute):
                receiver = node.func.value
                constructor = (inv._resolved_dotted(receiver.func, self.aliases)
                    if isinstance(receiver, ast.Call) else
                    self.receivers.get(receiver.id) if isinstance(receiver, ast.Name) else None)
                if constructor == 'urllib.request.build_opener' and node.func.attr == 'open':
                    primitive = 'census_http_chained_open'
                elif constructor == 'requests.Session' and node.func.attr in (
                        'get', 'post', 'put', 'delete', 'head', 'patch', 'request', 'send'):
                    primitive = 'census_http_session_request'
                elif constructor in ('ftplib.FTP', 'ftplib.FTP_TLS') and node.func.attr in (
                        'connect', 'retrbinary', 'retrlines', 'storbinary', 'storlines'):
                    primitive = 'census_ftp_transfer'
            if primitive is not None and inv._primitive(node, self.aliases) is None:
                qualname = '.'.join(self.stack) if self.stack else '<module>'
                self.candidates.append(dict(module=module, qualname=qualname,
                    caller_chain=list(self.stack), primitive=primitive, line=node.lineno,
                    column=node.col_offset, call=' '.join(ast.get_source_segment(source, node).split())))
            self.generic_visit(node)
    visitor = ResidualVisitor()
    visitor.visit(ast.parse(source, filename=module))
    ordinals = {}
    for row in sorted(visitor.candidates, key=lambda row: (row['line'], row['column'])):
        group = row['qualname'], row['primitive']
        ordinals[group] = ordinals.get(group, 0) + 1
        row['site_key'] = f"{module}:{group[0]}:{group[1]}:{ordinals[group]}"
        rows.append(row)
    return sorted(rows, key=lambda row: (row['line'], row['column']))


def _excluded_call_matches(actual, expected):
    """Exact source custody, without copying executable child code as a literal."""
    if isinstance(expected, str):
        return actual == expected
    if (type(expected) is not tuple or len(expected) != 2 or expected[0] != "sha256"
            or not isinstance(expected[1], str) or len(expected[1]) != 64
            or any(c not in "0123456789abcdef" for c in expected[1])):
        raise inv.InventoryError("invalid excluded call digest")
    return hashlib.sha256(actual.encode("utf-8")).hexdigest() == expected[1]


def check_excluded_census(sources, dispositions=_EXCLUDED_SEND_DISPOSITIONS,
                          proofs=_EXCLUDED_PROOF_SOURCES):
    expected = {}
    for key, path, call, disposition in dispositions:
        if key in expected:
            raise inv.InventoryError("duplicate excluded disposition: " + key)
        if path not in proofs or disposition != proofs[path][1]:
            raise inv.InventoryError("unbound excluded disposition: " + key)
        expected[key] = (path, call, disposition)
    rows, excluded_sources, trees = [], {}, {}
    for relative, source in sorted(sources.items()):
        path = Path(relative)
        # The existing production gate already scans the complementary paths.
        # Reconcile precisely its omitted universe, without re-parsing the
        # very large included engine files for every excluded-source inverse.
        if not (path.name in inv.EXCLUDED_NAMES or path.name.endswith(inv.EXCLUDED_SUFFIXES)):
            continue
        module = ".".join(path.with_suffix("").parts)
        candidates, tree = _excluded_scan_cached(module, source, scan_excluded_source, ast.parse)
        trees[relative] = tree
        if path.name in inv.EXCLUDED_NAMES or path.name.endswith(inv.EXCLUDED_SUFFIXES):
            excluded_sources[relative] = source
            for record in candidates:
                row = dict(record)
                key = row["site_key"]
                if key not in expected:
                    raise inv.InventoryError("unknown excluded sender: " + key)
                owner, call, disposition = expected[key]
                if relative != owner or not _excluded_call_matches(row["call"], call):
                    raise inv.InventoryError("excluded site binding changed: " + disposition + ":" + key)
                rows.append(dict(row, path=relative, disposition=disposition))
    reached = {row["site_key"] for row in rows}
    missing = set(expected) - reached
    if missing:
        raise inv.InventoryError("missing excluded sender: " + sorted(missing)[0])
    if set(proofs) != {path for path, _, _ in expected.values()}:
        raise inv.InventoryError("excluded proof/owner set mismatch")
    for relative, (digest, disposition, evidence) in proofs.items():
        # Defensive backstop: an absent sender-owning source already fails the
        # missing-key check above; every proof owner must own an expected row.
        if relative not in excluded_sources:
            raise inv.InventoryError("missing excluded proof: " + disposition + ":" + relative)
        source = excluded_sources[relative]
        normalized_source = source.replace("\r\n", "\n")
        tree = trees[relative]
        if disposition == "PHASE0_MUST_BE_HELD":
            fetch = next((node for node in tree.body if isinstance(node, ast.FunctionDef)
                          and node.name == "_fetch_bytes"), None)
            first = ast.dump(ast.parse("refuse_a2()").body[0], include_attributes=False)
            if fetch is None or not fetch.body or ast.dump(fetch.body[0], include_attributes=False) != first:
                raise inv.InventoryError("PHASE0_MUST_BE_HELD first-statement refusal missing")
        if disposition in {"STOCK_FAKE_AND_LOOPBACK", "DISCOVERY_LOOPBACK"}:
            binds = [node for node in ast.walk(tree) if isinstance(node, ast.Call)
                     and isinstance(node.func, ast.Attribute) and node.func.attr == "bind"]
            count = 1 if disposition == "STOCK_FAKE_AND_LOOPBACK" else 2
            try:
                bound = [ast.literal_eval(node.args[0]) for node in binds]
            except (ValueError, TypeError, IndexError) as exc:
                raise inv.InventoryError(disposition + " explicit loopback binding lost") from exc
            if bound != [("127.0.0.1", 0)] * count:
                raise inv.InventoryError(disposition + " explicit loopback binding changed")
        for anchor in evidence:
            if anchor.replace("\r\n", "\n") not in normalized_source:
                raise inv.InventoryError("source-bound boundary missing: " + disposition + ":" + relative)
        normalized = normalized_source.encode("utf-8")
        if hashlib.sha256(normalized).hexdigest() != digest:
            raise inv.InventoryError("source-bound proof changed: " + disposition + ":" + relative)
    for relative, digest in _EXCLUDED_DEPENDENCY_SHA256.items():
        if relative not in sources or hashlib.sha256(
                sources[relative].replace("\r\n", "\n").encode("utf-8")).hexdigest() != digest:
            raise inv.InventoryError("excluded confined-policy dependency changed: " + relative)
    return sorted(rows, key=lambda row: row["site_key"])


def excluded_walk_witness(walker):
    expected_paths = (
        "ordinary.py", "new_reference.py", "new_selftest.py", "run_gates.py",
        "engine/ordinary.py", "engine/new_reference.py",
        "engine/nested/ordinary.py", "engine/nested/new_selftest.py",
        "tools/ordinary.py", "tools/new_reference.py",
        "tools/nested/ordinary.py", "tools/nested/new_selftest.py",
        "ops/ordinary.py", "ops/new_reference.py",
        "ops/nested/ordinary.py", "ops/nested/new_selftest.py",
    )
    expected = {name: "VALUE = " + repr(name) + "\n" for name in expected_paths}
    with tempfile.TemporaryDirectory(prefix="row79-census-walk-") as folder:
        root = Path(folder)
        for name, source in expected.items():
            path = root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(source, encoding="utf-8")
        (root / "ignored.txt").write_text("not Python", encoding="utf-8")
        (root / "unshipped").mkdir()
        (root / "unshipped" / "new_reference.py").write_text("ignored = True\n", encoding="utf-8")
        observed = walker(root)
        assert observed == expected, "actual excluded walk path/text mapping differs: " + repr(
            sorted(set(expected) - set(observed)))
    return True


def excluded_walk_escape_witness(walker):
    with tempfile.TemporaryDirectory(prefix="row79-census-escape-") as folder:
        root = Path(folder).resolve()
        source = root / "new_reference.py"
        source.write_text("fixture = True\n", encoding="utf-8")
        original_resolve = Path.resolve
        def fixture_resolve(path, *args, **kwargs):
            if path == source:
                return root.parent / "outside-census-source.py"
            return original_resolve(path, *args, **kwargs)
        with patch.object(Path, "resolve", fixture_resolve):
            try:
                walker(root)
            except inv.InventoryError as exc:
                assert str(exc).startswith("excluded census source leaves project:"), str(exc)
            else:
                raise AssertionError("escaped fixture source was not refused")
    return True


def excluded_function_inverse(function, old, new):
    source = inspect.getsource(function)
    assert source.count(old) == 1, "excluded inverse anchor must match exactly once"
    namespace = globals().copy()
    exec(compile(source.replace(old, new, 1), function.__code__.co_filename, "exec"), namespace)
    return namespace[function.__name__]


section("actual excluded tree walk and unchanged-text analysis controls")
check("actual excluded tree walk returns all sixteen root/shipped/nested sources",
      excluded_walk_witness(excluded_source_universe))
walk_mutations = [
    ("root", 'paths = list(project_root.glob("*.py"))', 'paths = []'),
    ("recursion", '.rglob("*.py")', '.glob("*.py")'),
    ("only-engine", 'for directory in inv.SHIPPED_SUBDIRS:', 'for directory in ("engine",):'),
]
for omitted in inv.SHIPPED_SUBDIRS:
    walk_mutations.append(("omit-" + omitted, 'for directory in inv.SHIPPED_SUBDIRS:',
        'for directory in tuple(name for name in inv.SHIPPED_SUBDIRS if name != ' + repr(omitted) + '):'))
for name, old, new in walk_mutations:
    mutant = excluded_function_inverse(excluded_source_universe, old, new)
    try:
        excluded_walk_witness(mutant)
    except AssertionError as exc:
        check("actual excluded walk omission inverse red: " + name,
              "actual excluded walk path/text mapping differs:" in str(exc), str(exc))
    else:
        check("actual excluded walk omission inverse red: " + name, False, "walk omission survived")
check("actual excluded walk refuses a path resolved outside the fixture",
      excluded_walk_escape_witness(excluded_source_universe))
escaped_mutant = excluded_function_inverse(excluded_source_universe,
    'if not path.resolve().is_relative_to(project_root.resolve()):', 'if False:')
try:
    excluded_walk_escape_witness(escaped_mutant)
except AssertionError as exc:
    check("actual excluded walk escape-guard inverse red",
          str(exc) == "escaped fixture source was not refused", str(exc))
else:
    check("actual excluded walk escape-guard inverse red", False, "escape guard removal survived")
cache_scanner, cache_parser = Mock(wraps=inv.scan_source), Mock(wraps=ast.parse)
cache_text = "fixture = 1\n"
cache_first = _excluded_scan_cached("engine.cache_reference", cache_text, cache_scanner, cache_parser)
check("excluded analysis reuses only identical source and current functions",
      _excluded_scan_cached("engine.cache_reference", cache_text, cache_scanner, cache_parser) is cache_first
      and cache_scanner.call_count == cache_parser.call_count == 1)
_excluded_scan_cached("engine.cache_reference", cache_text + "# changed\n", cache_scanner, cache_parser)
check("excluded analysis changed source is reparsed",
      cache_scanner.call_count == cache_parser.call_count == 2)
cache_scanner_changed = Mock(wraps=inv.scan_source)
_excluded_scan_cached("engine.cache_reference", cache_text, cache_scanner_changed, cache_parser)
check("excluded analysis changed scanner is recomputed",
      cache_scanner_changed.call_count == 1 and cache_parser.call_count == 3)
cache_parser_changed = Mock(wraps=ast.parse)
_excluded_scan_cached("engine.cache_reference", cache_text, cache_scanner, cache_parser_changed)
check("excluded analysis changed parser is recomputed and cache is finite",
      cache_scanner.call_count == 3 and cache_parser_changed.call_count == 1
      and _excluded_scan_cached.cache_info().maxsize == 256)


section("standing excluded sender census — AST only, no harness execution")
excluded_sources = excluded_source_universe(ENGINE_ROOT.parent)
excluded_rows = check_excluded_census(excluded_sources)
check("95 excluded exact keys remain separate from 71 production candidates",
      len(excluded_rows) == 95 and inventory["candidate_count"] == 71
      and not ({row["site_key"] for row in excluded_rows} & {row["site_key"] for row in sites}))
check("25 exact excluded proof owners and one confined-policy dependency",
      len(_EXCLUDED_PROOF_SOURCES) == 25 and len(_EXCLUDED_DEPENDENCY_SHA256) == 1
      and {row["path"] for row in excluded_rows} == set(_EXCLUDED_PROOF_SOURCES))
for key, path, call, disposition in _EXCLUDED_SEND_DISPOSITIONS:
    check("excluded exact binding: " + key,
          any((row["site_key"], row["path"], row["disposition"]) == (key, path, disposition)
              and _excluded_call_matches(row["call"], call) for row in excluded_rows))


def excluded_changed(path, old, new):
    source = excluded_sources[path]
    if source.count(old) != 1:
        raise AssertionError("excluded inverse anchor must match exactly once: " + path)
    return dict(excluded_sources, **{path: source.replace(old, new, 1)})


_DAILY_PROOF = "engine/daily_bar_repair_selftest.py"
for label, old, new in (
    ("guard", "self.guard = policy.offline_transports()", "self.guard = nullcontext()"),
    ("fake", "self.raw = self.adapter.ib = RawIB([bar(DAY)])", "self.raw = self.adapter.ib"),
    ("child target", 'base = [sys.executable, "-B", str(Path(__file__).resolve())]',
                     'base = [sys.executable, "-B", "foreign.py"]'),
    ("fixture root", "supplied.parent != root.resolve()", "False"),
):
    expect_error("daily repair excluded proof refuses lost " + label,
                 "source-bound boundary missing: DAILY_REPAIR_CONFINED_AND_FIXED_CHILDREN:",
                 lambda old=old, new=new: check_excluded_census(
                     excluded_changed(_DAILY_PROOF, old, new)))


_HORIZON_SITE = "engine.fetch_horizon_selftest:<module>:census_process_launch:1"
_HORIZON_SUFFIX = "cwd=relocated, capture_output=True, text=True, timeout=30)"
_HORIZON_CHANGED = excluded_changed("engine/fetch_horizon_selftest.py",
    _HORIZON_SUFFIX, "cwd=relocated, capture_output=False, text=True, timeout=30)")
expect_error("excluded hashed child call drift refuses exact binding before whole-source pin",
    "excluded site binding changed: LOCAL_HARNESS_CHILD:" + _HORIZON_SITE,
    lambda: check_excluded_census(_HORIZON_CHANGED))
_HORIZON_EXPECTED = next(call for key, _, call, _ in _EXCLUDED_SEND_DISPOSITIONS
                        if key == _HORIZON_SITE)
_HORIZON_ACTUAL = next(row["call"] for row in excluded_rows if row["site_key"] == _HORIZON_SITE)
check("excluded hashed call independently matches the actual AST segment",
      _excluded_call_matches(_HORIZON_ACTUAL, _HORIZON_EXPECTED))
check("excluded hashed call independently rejects changed actual call",
      not _excluded_call_matches(_HORIZON_ACTUAL.replace("capture_output=True",
                                                      "capture_output=False"), _HORIZON_EXPECTED))
_HASH_MUTANT = excluded_function_inverse(_excluded_call_matches,
    'return hashlib.sha256(actual.encode("utf-8")).hexdigest() == expected[1]', 'return True')
check("excluded call-digest comparison removal intended drift oracle red",
      _HASH_MUTANT(_HORIZON_ACTUAL.replace("capture_output=True",
                                        "capture_output=False"), _HORIZON_EXPECTED))


for path in ("engine/split_provider_reference.py", "engine/fetch_pickup_selftest.py"):
    unknown = dict(excluded_sources)
    unknown[path] += '\nimport urllib.request\ndef escaped_sender():\n    urllib.request.urlopen("https://example.invalid/")\n'
    expect_error("excluded new outward open fails by exact key: " + path,
                 "unknown excluded sender: " + path[:-3].replace("/", ".") + ":escaped_sender:http_urlopen:1",
                 lambda data=unknown: check_excluded_census(data))
expect_error("excluded Phase0 removed hold fails its named boundary",
             "PHASE0_MUST_BE_HELD first-statement refusal missing",
             lambda: check_excluded_census(excluded_changed(
                 "engine/split_provider_reference.py", "    refuse_a2()\n", "")))
expect_error("excluded fake-to-real receiver fails its named site binding",
             "excluded site binding changed: WATCHDOG_FAKE_ADAPTER:",
             lambda: check_excluded_census(excluded_changed(
                 "engine/addstock_watchdog_selftest.py",
                 "            adapter = adapter_factory()\n            adapter.fetch()\n",
                 "            adapter = adapter_factory()\n            sk.LiveIB().fetch()\n")))
for path, disposition in (("engine/tws_discovery_reference.py", "DISCOVERY_LOOPBACK"),
                          ("engine/stock_ibkr_selftest.py", "STOCK_FAKE_AND_LOOPBACK")):
    changed = dict(excluded_sources)
    changed[path] = changed[path].replace('bind(("127.0.0.1", 0))', 'bind(("0.0.0.0", 0))', 1)
    expect_error("excluded outward listener fails named loopback boundary: " + disposition,
                 disposition + " explicit loopback binding changed",
                 lambda data=changed: check_excluded_census(data))
expect_error("excluded deleting a row refuses its exact still-shipped key",
             "unknown excluded sender: " + _EXCLUDED_SEND_DISPOSITIONS[0][0],
             lambda: check_excluded_census(excluded_sources, _EXCLUDED_SEND_DISPOSITIONS[1:]))
rekeyed = list(_EXCLUDED_SEND_DISPOSITIONS)
rekeyed[0] = ("engine.rekeyed:wrong:adapter_fetch:1", *rekeyed[0][1:])
expect_error("excluded rekeying a row refuses its exact still-shipped key",
             "unknown excluded sender: " + _EXCLUDED_SEND_DISPOSITIONS[0][0],
             lambda: check_excluded_census(excluded_sources, rekeyed))
expect_error("excluded duplicate disposition refuses instead of dict overwrite",
             "duplicate excluded disposition: " + _EXCLUDED_SEND_DISPOSITIONS[0][0],
             lambda: check_excluded_census(excluded_sources, (*_EXCLUDED_SEND_DISPOSITIONS,
                                                            _EXCLUDED_SEND_DISPOSITIONS[0])))
wrong_disposition = list(_EXCLUDED_SEND_DISPOSITIONS)
wrong_disposition[0] = (*wrong_disposition[0][:3], "WRONG_OWNER_DISPOSITION")
expect_error("excluded row disposition must equal its proof owner's disposition",
             "unbound excluded disposition: " + _EXCLUDED_SEND_DISPOSITIONS[0][0],
             lambda: check_excluded_census(excluded_sources, wrong_disposition))
unowned_proof = dict(_EXCLUDED_PROOF_SOURCES, **{
    "engine/unowned_reference.py": ("0" * 64, "NO_OWNED_ROW", ("fixture",)),
})
expect_error("excluded proof owner without a table row refuses named set mismatch",
             "excluded proof/owner set mismatch",
             lambda: check_excluded_census(excluded_sources, proofs=unowned_proof))
expect_error("excluded fake superclass change fails source-bound proof",
             "source-bound boundary missing: WINDOW_FAKE_ADAPTER:",
             lambda: check_excluded_census(excluded_changed(
                 "engine/extended_hours_selftest.py", "class _WindowAdapter(ExtAdapter):",
                 "class _WindowAdapter(sk.LiveIB):")))
expect_error("excluded proof-only drift fails named source custody",
             "source-bound proof changed: PICKUP_FAKE_ADAPTER:",
             lambda: check_excluded_census(dict(excluded_sources, **{
                 "engine/fetch_pickup_selftest.py": excluded_sources["engine/fetch_pickup_selftest.py"] + "\n# drift\n"})))
expect_error("excluded changed shared transport guard fails dependency custody",
             "excluded confined-policy dependency changed:",
             lambda: check_excluded_census(dict(excluded_sources, **{
                 "engine/fetch_test_policy.py": excluded_sources["engine/fetch_test_policy.py"] + "\n# drift\n"})))
expect_error("excluded removed shipped source refuses exact missing key",
             "missing excluded sender: " + min(key for key, path, _, _ in _EXCLUDED_SEND_DISPOSITIONS
                                               if path == "engine/fetch_pickup_selftest.py"),
             lambda: check_excluded_census({path: source for path, source in excluded_sources.items()
                                           if path != "engine/fetch_pickup_selftest.py"}))
expect_error("excluded malformed source fails AST parse closed", "cannot parse",
             lambda: check_excluded_census(dict(excluded_sources, **{"engine/broken_reference.py": "def broken(:\n"})))
with patch.object(Path, "read_text", side_effect=OSError("counted unreadable source")):
    expect_error("excluded unreadable shipped source fails closed", "cannot read excluded census source:",
                 lambda: excluded_source_universe(ENGINE_ROOT.parent))
for path in ("new_reference.py", "new_selftest.py", "run_gates.py",
             "engine/new_reference.py", "engine/nested/new_selftest.py",
             "tools/new_reference.py", "tools/nested/new_selftest.py",
             "ops/new_reference.py", "ops/nested/new_selftest.py"):
    unknown = dict(excluded_sources, **{path: 'import urllib.request\nurllib.request.urlopen("https://example.invalid/")\n'})
    expect_error("excluded universe covers name/suffix omission: " + path,
                 "unknown excluded sender: " + path[:-3].replace("/", ".") + ":<module>:http_urlopen:1",
                 lambda data=unknown: check_excluded_census(data))
check("excluded proof custody survives CRLF-equivalent copy",
      check_excluded_census({path: source.replace("\r\n", "\n").replace("\n", "\r\n")
                             for path, source in excluded_sources.items()}) == excluded_rows)
section("census-only residual send and process spellings")
_RESIDUAL_FIXTURES = (
    ('retrieve', 'import urllib.request\nurllib.request.urlretrieve("https://example.invalid/")\n', 'census_http_retrieve'),
    ('retrieve-import-alias', 'from urllib.request import urlretrieve as take\ntake("https://example.invalid/")\n', 'census_http_retrieve'),
    ('chained-open', 'import urllib.request\nurllib.request.build_opener().open("https://example.invalid/")\n', 'census_http_chained_open'),
    ('assigned-open', 'import urllib.request as u\nopener=u.build_opener()\nopener.open("https://example.invalid/")\n', 'census_http_chained_open'),
    ('dns', 'import socket\nsocket.gethostbyname("example.invalid")\n', 'census_dns_lookup'),
    ('async', 'import asyncio\nasyncio.open_connection("example.invalid",443)\n', 'census_async_connect'),
    ('session', 'import requests\nrequests.Session().get("https://example.invalid/")\n', 'census_http_session_request'),
    ('assigned-session', 'import requests as r\nclient=r.Session()\nclient.get("https://example.invalid/")\n', 'census_http_session_request'),
    ('process', 'import subprocess\nsubprocess.run(["curl","https://example.invalid/"])\n', 'census_process_launch'),
    ('process-import-alias', 'from subprocess import Popen as spawn\nspawn(["curl","https://example.invalid/"])\n', 'census_process_launch'),
    ('os-system', 'import os\nos.system("curl https://example.invalid/")\n', 'census_process_launch'),
    ('os-system-module-alias', 'import os as o\no.system("curl https://example.invalid/")\n', 'census_process_launch'),
    ('os-system-import-alias', 'from os import system as launch\nlaunch("curl https://example.invalid/")\n', 'census_process_launch'),
    ('getoutput', 'import subprocess\nsubprocess.getoutput("curl https://example.invalid/")\n', 'census_process_launch'),
    ('getoutput-module-alias', 'import subprocess as p\np.getoutput("curl https://example.invalid/")\n', 'census_process_launch'),
    ('getoutput-import-alias', 'from subprocess import getoutput as launch\nlaunch("curl https://example.invalid/")\n', 'census_process_launch'),
    ('async-process', 'import asyncio\nasyncio.create_subprocess_exec("curl","https://example.invalid/")\n', 'census_process_launch'),
    ('async-process-module-alias', 'import asyncio as a\na.create_subprocess_exec("curl","https://example.invalid/")\n', 'census_process_launch'),
    ('async-process-import-alias', 'from asyncio import create_subprocess_exec as launch\nlaunch("curl","https://example.invalid/")\n', 'census_process_launch'),
    ('ftp', 'import ftplib\nftplib.FTP("example.invalid")\n', 'census_ftp_open'),
    ('ftp-transfer', 'import ftplib as f\nclient=f.FTP()\nclient.retrbinary("RETR data",lambda raw:None)\n', 'census_ftp_transfer'),
)
_A59_PROCESS_SPELLINGS = (
    'asyncio.create_subprocess_shell', 'subprocess.getstatusoutput', 'os.popen', 'os.startfile',
    'os.spawnl', 'os.spawnle', 'os.spawnlp', 'os.spawnlpe',
    'os.spawnv', 'os.spawnve', 'os.spawnvp', 'os.spawnvpe',
    'os.execl', 'os.execle', 'os.execlp', 'os.execlpe',
    'os.execv', 'os.execve', 'os.execvp', 'os.execvpe', 'os.posix_spawn', 'os.posix_spawnp',
)
for spelling in _A59_PROCESS_SPELLINGS:
    owner, name = spelling.split('.')
    _RESIDUAL_FIXTURES += (
        ('a59-' + spelling, f'import {owner}\n{spelling}("fixture-command")\n', 'census_process_launch'),
        ('a59-' + spelling + '-module-alias', f'import {owner} as alias\nalias.{name}("fixture-command")\n', 'census_process_launch'),
        ('a59-' + spelling + '-import-alias', f'from {owner} import {name} as launch\nlaunch("fixture-command")\n', 'census_process_launch'),
    )
for name, source, primitive in _RESIDUAL_FIXTURES:
    candidates = scan_excluded_source(source, 'engine.residual_reference')
    check('residual detected literal/alias receiver: ' + name,
          any(row['primitive'] == primitive for row in candidates))
    unknown = dict(excluded_sources, **{'engine/residual_reference.py': source})
    expect_error('residual unknown candidate fails its exact key: ' + name,
                 'unknown excluded sender: engine.residual_reference:<module>:',
                 lambda data=unknown: check_excluded_census(data))
for old, fixture_name in (
        ("if name == 'urllib.request.urlretrieve':", 'retrieve'),
        ("elif name in ('socket.gethostbyname', 'socket.gethostbyname_ex'):", 'dns'),
        ("elif name == 'asyncio.open_connection':", 'async'),
        ("elif name in ('subprocess.run', 'subprocess.Popen', 'subprocess.call',", 'process'),
        ("elif name in ('ftplib.FTP', 'ftplib.FTP_TLS'):", 'ftp'),
        ("if constructor == 'urllib.request.build_opener' and node.func.attr == 'open':", 'chained-open'),
        ("elif constructor == 'requests.Session' and node.func.attr in (", 'session')):
    # Removing a detector must lose the reached positive, not merely change
    # the final census count. Never execute the synthetic outward commands.
    if fixture_name == 'process':
        new = "elif name in ('NOT_SUBPROCESS', 'subprocess.Popen', 'subprocess.call',"
    elif fixture_name == 'session':
        new = "elif constructor == 'NOT_SESSION' and node.func.attr in ("
    else:
        new = old.replace('if ', 'if False and ', 1)
    mutant = excluded_function_inverse(scan_excluded_source, old, new)
    source, primitive = next((src, prim) for name, src, prim in _RESIDUAL_FIXTURES if name == fixture_name)
    check('residual detector removal intended positive red: ' + fixture_name,
          not any(row['primitive'] == primitive for row in mutant(source, 'engine.residual_reference')))
for spelling, fixture_name in (('os.system', 'os-system'), ('subprocess.getoutput', 'getoutput'),
                                ('asyncio.create_subprocess_exec', 'async-process'),
                                *((name, 'a59-' + name) for name in _A59_PROCESS_SPELLINGS)):
    mutant = excluded_function_inverse(scan_excluded_source, repr(spelling), repr('NOT_' + spelling))
    for name, source, primitive in _RESIDUAL_FIXTURES:
        if name == fixture_name or name.startswith(fixture_name + '-'):
            check('new process spelling removal loses direct/alias positive: ' + name,
                  not any(row['primitive'] == primitive for row in mutant(source, 'engine.residual_reference')))
print("EXCLUDED_CENSUS_RECEIPT", json.dumps(dict(candidates=len(excluded_rows),
    proof_owners=len(_EXCLUDED_PROOF_SOURCES), dependency_sources=len(_EXCLUDED_DEPENDENCY_SHA256),
    production_candidates=inventory["candidate_count"], harness_executions=0,
    keys=[row["site_key"] for row in excluded_rows]), sort_keys=True))


raise SystemExit(KIT.finish())
