"""Offline contract tests for the Row 79 A1 send inventory."""

from __future__ import annotations

import ast
import inspect
import json
from pathlib import Path
import sys
import tempfile
from unittest.mock import patch


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
check("candidate denominator is 71 including the default HTTP route and four connection helpers",
      inventory["candidate_count"] == 71,
      str(inventory["candidate_count"]))
check("logical producer denominator is 60 including the five owned HTTP mechanics",
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
check("five raw HTTP sites include the diagnostic control probe",
      inventory["primitive_counts"].get("http_urlopen") == 5,
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
      inventory["primitive_counts"].get("http_opener_open") == 1
      and inventory["primitive_counts"].get("socket_dns") == 1)
check("candidate surface digest is canonical lowercase SHA-256",
      len(inventory["candidate_surface_sha256"]) == 64
      and inventory["candidate_surface_sha256"].isalnum()
      and inventory["candidate_surface_sha256"].lower()
      == inventory["candidate_surface_sha256"])
check("A2-3a exact source-surface digest is pinned",
      inventory["candidate_surface_sha256"]
      == "97e55c2e20171b01bfc22fb23c09ed000b4945ac3eb04aec395d247506a2660a")
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
check("all six HTTP data/provider sites are A2-refused",
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


raise SystemExit(KIT.finish())
