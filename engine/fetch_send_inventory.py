"""Deterministic, fail-closed inventory of shipped fetch/network send sites.

The Row 79 A1 transport guard is only as strong as its denominator.  This
module scans shipped root modules and ``engine/``, ``tools/``, and ``ops/``
Python sources with the stdlib AST,
assigns line-independent site keys, and requires an explicit reviewed
classification for every candidate before it can emit the committed artifact.

Method names are candidates regardless of receiver spelling. Literal getattr
and import aliases are recognized; computed names, method-reference aliases,
and unrecognized network libraries still require review and runtime guards.
"""

from __future__ import annotations

import argparse
import ast
from collections import Counter, defaultdict
import hashlib
import json
from pathlib import Path
import sys
from typing import Iterable, Mapping


SCHEMA_VERSION = 1
ENGINE_ROOT = Path(__file__).resolve().parent
INVENTORY_PATH = ENGINE_ROOT / "fetch_send_inventory.json"
SHIPPED_SUBDIRS = ("engine", "tools", "ops")
EXCLUDED_SUFFIXES = ("_selftest.py", "_reference.py")
EXCLUDED_NAMES = frozenset({"run_gates.py"})
# Receiver names are not type evidence. Some matches are adapter wrappers or
# socket control calls; each still requires a reviewed, site-specific row.
IBKR_NON_REQ_METHODS = frozenset({
    "qualifyContracts", "qualifyContractsAsync", "connect", "connectAsync",
    "managedAccounts", "placeOrder", "cancelOrder", "whatIfOrder", "whatIfOrderAsync",
})
# O1 correction: the nightly path reaches two LOCAL sidecar helpers in a
# module that also has an HTTP seam. Only these reviewed source bodies (and
# their import bindings below) are exceptions, never the whole module.
A1_OFFLINE_HELPER_SHA256 = {
    "engine.stock_validate.load_ibkr_earliest": "9af1173a1dd8428e0427c8b883bf4bd864299154fab88bc22c5b6cc3b22a2904",
    "engine.stock_validate.record_ibkr_earliest": "ae6fbe5ade9bb4d4ab0dd352fb0ebc0f08da235858e1f2375d6beac79ab3277a",
}


class InventoryError(ValueError):
    """The source tree cannot produce one unambiguous reviewed inventory."""


def _site(producer_id: str, transport: str, disposition: str,
          reason: str) -> Mapping[str, object]:
    return {
        "producer_id": producer_id,
        "transport": transport,
        "disposition": disposition,
        "reason": reason,
    }


_A2_IBKR = "A2 path; refused at the guarded LiveIB choke point until A2 approval"
_A2_HTTP = "A2 HTTP path; refused at its seam switch until A2 approval"
_A1_SHARED = (
    "reachable from gap_fill_parallel only with its A1-bound context; shared "
    "legacy callers remain refused"
)
_CONTROL_SOCKET = "control-plane port probe only; no market-data response is consumed"
_CONTROL_CONNECT = (
    "connection setup or adapter connection wrapper only; no market-data response "
    "is consumed; downstream data sends retain their own guards"
)
_A1_QUALIFY = (
    "shared contract-qualification choke point; A1-bound context and ledger-only "
    "ibkr-metadata required on the nightly path (spec section 4); other callers "
    "remain refused until A2"
)


# Every candidate is named here.  There is intentionally no wildcard/default:
# a new, moved, removed, or re-keyed site makes regeneration fail closed.
SITE_CLASSIFICATIONS: Mapping[str, Mapping[str, object]] = {
    "engine.fetch_http_labels:guarded_stockanalysis_attempt.one_send:http_opener_open:1": _site(
        "http.stockanalysis.owned_default", "http-series", "a2_refused",
        "shared owned physical open; validation/sweep worker, admission, turn and durable decision precede it; A2 remains held"),
    "engine.fetch_http_transport:_lookup_addresses:socket_dns:1": _site(
        "http.stockanalysis.dns", "http-control", "a2_refused",
        "fixed StockAnalysis DNS only; single outstanding lookup, bounded caller wait, no background request or send"),
    "engine.fetch_http_transport:_make_socket:socket:1": _site(
        "http.stockanalysis.connection.socket", "http-control", "a2_refused",
        "request-owned socket under the same total deadline; only the guarded default open reaches this helper"),
    "engine.fetch_http_transport:connect_resolved:ibkr_non_req:1": _site(
        "http.stockanalysis.connection.connect", "http-control", "a2_refused",
        "fixed-host resolved TCP setup; deadline-bound address fallback sends no HTTP request; socket tripwires retained"),
    "engine.fetch_http_transport:DeadlineHTTPSConnection.connect:ibkr_non_req:1": _site(
        "http.stockanalysis.connection.tls", "http-control", "a2_refused",
        "stdlib HTTPS connect with verified context and remaining handshake budget; parent connect tripwires retained"),
    "display_data:DataViewerApp._fixdata_start._engine_run._adapter:ibkr_non_req:1": _site(
        "ibkr.control.display_data.fixdata", "ibkr-control", "control_message", _CONTROL_CONNECT),
    "engine.live_combined_flags:_connect:ibkr_non_req:1": _site(
        "ibkr.control.live_combined_flags", "ibkr-control", "control_message", _CONTROL_CONNECT),
    "engine.live_feature_check:_feature_body:ibkr_non_req:1": _site(
        "ibkr.control.live_feature_check", "ibkr-control", "control_message", _CONTROL_CONNECT),
    "engine.live_feature_check:_feature_body:ibkr_non_req:2": _site(
        "ibkr.control.live_feature_check", "ibkr-control", "control_message", _CONTROL_CONNECT),
    "engine.live_feature_check:_feature_body:ibkr_non_req:3": _site(
        "ibkr.control.live_feature_check", "ibkr-control", "control_message", _CONTROL_CONNECT),
    "engine.live_internal_revalidate:_connect:ibkr_non_req:1": _site(
        "ibkr.control.live_internal_revalidate", "ibkr-control", "control_message", _CONTROL_CONNECT),
    "engine.live_kind_smoke:connect:ibkr_non_req:1": _site(
        "ibkr.control.live_kind_smoke", "ibkr-control", "control_message", _CONTROL_CONNECT),
    "engine.live_spot_probe:IBKRMinuteDayFetcher.start:ibkr_non_req:1": _site(
        "ibkr.control.live_spot_probe", "ibkr-control", "control_message", _CONTROL_CONNECT),
    "engine.live_validate_catchup:_catchup_port:ibkr_non_req:1": _site(
        "ibkr.control.live_validate_catchup", "ibkr-control", "control_message", _CONTROL_CONNECT),
    "engine.row82_vol_reconcile_run:live_ports:ibkr_non_req:1": _site(
        "socket.row82.live_ports", "socket-control", "control_socket", _CONTROL_SOCKET),
    "engine.stock_ibkr:LiveIB._connect_under_gate:ibkr_non_req:1": _site(
        "ibkr.control.connect", "ibkr-control", "control_message", _CONTROL_CONNECT),
    "engine.stock_ibkr:LiveIB.account:ibkr_non_req:1": _site(
        "ibkr.control.account", "ibkr-control", "control_message",
        "reads the connected adapter's managed-account identities; no market data is requested or consumed"),
    "engine.stock_ibkr:LiveIB.reconnect:ibkr_non_req:1": _site(
        "ibkr.control.reconnect", "ibkr-control", "control_message", _CONTROL_CONNECT),
    "engine.stock_ibkr:live_adapter_factory.make:ibkr_non_req:1": _site(
        "ibkr.control.adapter_factory", "ibkr-control", "control_message", _CONTROL_CONNECT),
    "engine.two_account_pacing_probe:_connect.make:ibkr_non_req:1": _site(
        "ibkr.control.two_account_pacing_probe", "ibkr-control", "control_message", _CONTROL_CONNECT),
    "engine.tws_launch:port_open:ibkr_non_req:1": _site(
        "socket.tws_launch.port_open", "socket-control", "control_socket", _CONTROL_SOCKET),
    "engine.tws_launch:verify_handshake:ibkr_non_req:1": _site(
        "ibkr.control.verify_handshake", "ibkr-control", "control_message", _CONTROL_CONNECT),
    "display_data:print_diagnostics:http_urlopen:1": _site(
        "http.display_data.pypi_connectivity", "http-control", "control_probe",
        "diagnostic PyPI connectivity probe; reports HTTP status only and consumes no market data"),
    "engine.external_sweep:StockAnalysisProvider._read:http_urlopen:1": _site(
        "http.stockanalysis.external_sweep", "http-series", "a2_refused", _A2_HTTP),
    "engine.external_sweep:_provider_fetch:provider_fetch:1": _site(
        "http.stockanalysis.provider_dispatch", "http-series", "a2_refused",
        "logical provider dispatch for the A2 StockAnalysis seam; the raw send is separately inventoried"),
    "engine.live_combined_flags:_fetch_month_1d:adapter_fetch:1": _site(
        "ibkr.live_combined_flags.daily_refetch", "ibkr-bars", "a2_refused", _A2_IBKR),
    "engine.live_combined_flags:_fetch_month_1m:adapter_fetch:1": _site(
        "ibkr.live_combined_flags.minute_month", "ibkr-bars", "a2_refused", _A2_IBKR),
    "engine.live_internal_daily_check:_premise_request:adapter_fetch:1": _site(
        "ibkr.live_internal_daily_check.daily", "ibkr-bars", "a2_refused", _A2_IBKR),
    "engine.live_internal_revalidate:_internal_minute:adapter_fetch:1": _site(
        "ibkr.live_internal_revalidate.minute_refetch", "ibkr-bars", "a2_refused", _A2_IBKR),
    "engine.live_internal_revalidate:_internal_daily:adapter_fetch:1": _site(
        "ibkr.live_internal_revalidate.daily", "ibkr-bars", "a2_refused", _A2_IBKR),
    "engine.live_kind_smoke:test_hvol_daily:adapter_fetch:1": _site(
        "ibkr.live_kind_smoke.hvol_daily", "ibkr-bars", "a2_refused", _A2_IBKR),
    "engine.live_kind_smoke:test_hvol_daily:adapter_fetch:2": _site(
        "ibkr.live_kind_smoke.hvol_intraday", "ibkr-bars", "a2_refused", _A2_IBKR),
    "engine.live_kind_smoke:test_iv_rth_only:adapter_fetch:1": _site(
        "ibkr.live_kind_smoke.iv_rth", "ibkr-bars-unfiltered", "a2_refused", _A2_IBKR),
    "engine.live_kind_smoke:test_raw_daily_shape:adapter_fetch:1": _site(
        "ibkr.live_kind_smoke.iv_daily", "ibkr-bars", "a2_refused", _A2_IBKR),
    "engine.live_kind_smoke:test_raw_iv_intraday:adapter_fetch:1": _site(
        "ibkr.live_kind_smoke.iv_intraday", "ibkr-bars", "a2_refused", _A2_IBKR),
    "engine.live_kind_smoke:test_span_probe:adapter_fetch:1": _site(
        "ibkr.live_kind_smoke.span_probe", "ibkr-bars", "a2_refused", _A2_IBKR),
    "engine.live_spot_probe:IBKRDailySpanFetcher.fetch_span:adapter_fetch:1": _site(
        "ibkr.live_spot_probe.daily", "ibkr-bars", "a2_refused", _A2_IBKR),
    "engine.live_spot_probe:IBKRMinuteDayFetcher.fetch_day:adapter_fetch:1": _site(
        "ibkr.live_spot_probe.minute", "ibkr-bars", "a2_refused", _A2_IBKR),
    "engine.row82_vol_reconcile_run:live_ports:socket:1": _site(
        "socket.row82.live_ports", "socket-control", "control_socket", _CONTROL_SOCKET),
    "engine.sp500:_http:http_urlopen:1": _site(
        "http.sp500.membership", "http-metadata", "a2_refused", _A2_HTTP),
    "engine.split_provider:_default_fetcher:http_urlopen:1": _site(
        "http.split_provider.shared_sender", "http-mixed", "a2_refused",
        "A2 shared Yahoo/SEC/issuer sender; endpoint-specific envelopes are required before this switch lifts"),
    "engine.stock_ibkr:LiveIB.fetch.transport:ibkr_raw_bars:1": _site(
        "ibkr.choke.liveib_bars", "ibkr-bars", "guarded_choke_point",
        "the sole raw historical-bar send; A1 accepts only an A1-bound context and producer"),
    "engine.stock_ibkr:LiveIB.head_timestamp:ibkr_raw_head:1": _site(
        "ibkr.choke.liveib_head", "ibkr-head", "guarded_choke_point",
        "the sole raw head-timestamp send; A1 accepts only an A1-bound context and producer"),
    "engine.stock_ibkr:LiveIB._connect_under_gate:ibkr_raw_request:1": _site(
        "ibkr.control.market_data_type", "ibkr-control", "control_message",
        "connection setup selects delayed market-data mode; no data is requested or consumed"),
    "engine.stock_ibkr:LiveIB.company_name.transport:ibkr_raw_request:1": _site(
        "ibkr.metadata.company_name", "ibkr-metadata", "a2_refused",
        "A2 contract-details metadata request; requires its own guarded and ledgered sender before A2 approval"),
    "engine.stock_ibkr:LiveIB.search:ibkr_raw_request:1": _site(
        "ibkr.metadata.symbol_search", "ibkr-metadata", "a2_refused",
        "A2 symbol-search metadata request; requires its own guarded and ledgered sender before A2 approval"),
    "engine.stock_ibkr:LiveIB.qualify:ibkr_non_req:1": _site(
        "ibkr.choke.qualify", "ibkr-metadata", "guarded_choke_point", _A1_QUALIFY),
    "engine.stock_ibkr:LiveIB.qualify_many:ibkr_non_req:1": _site(
        "ibkr.choke.qualify_many", "ibkr-metadata", "guarded_choke_point", _A1_QUALIFY),
    "engine.stock_ibkr:LiveIB.qualify_many:ibkr_non_req:2": _site(
        "ibkr.choke.qualify_many", "ibkr-metadata", "guarded_choke_point", _A1_QUALIFY),
    "engine.stock_ibkr:_fetch_month_bars:adapter_fetch:1": _site(
        "ibkr.gap_fill.month_daily", "ibkr-bars", "a1_context_required", _A1_SHARED),
    "engine.stock_ibkr:_fetch_month_bars:adapter_fetch:2": _site(
        "ibkr.gap_fill.month_intraday", "ibkr-bars", "a1_context_required", _A1_SHARED),
    "engine.stock_ibkr:_fetch_request:adapter_fetch:1": _site(
        "ibkr.gap_fill.session_fetch", "ibkr-bars", "a1_context_required", _A1_SHARED),
    "engine.stock_ibkr:_fetch_request:adapter_fetch:2": _site(
        "ibkr.gap_fill.session_fetch", "ibkr-bars", "a1_context_required", _A1_SHARED),
    "engine.stock_ibkr:_head_start_evidence:adapter_head:1": _site(
        "ibkr.gap_fill.head_timestamp", "ibkr-head", "a1_context_required", _A1_SHARED),
    "engine.stock_ibkr:_head_start_evidence:adapter_head:2": _site(
        "ibkr.gap_fill.head_timestamp", "ibkr-head", "a1_context_required", _A1_SHARED),
    "engine.stock_ibkr:_identity_earliest_evidence:adapter_head:1": _site(
        "ibkr.identity.head_timestamp", "ibkr-head", "a2_refused",
        "A2 Add Stock identity probe; not reached by nightly gap fill; refused at the guarded head choke point"),
    "engine.stock_ibkr:_identity_earliest_evidence:adapter_head:2": _site(
        "ibkr.identity.head_timestamp", "ibkr-head", "a2_refused",
        "A2 Add Stock identity probe; not reached by nightly gap fill; refused at the guarded head choke point"),
    "engine.stock_ibkr:_port_open:socket:1": _site(
        "socket.stock_ibkr.port_open", "socket-control", "control_socket", _CONTROL_SOCKET),
    "engine.stock_ibkr:_probe_earliest_daily:adapter_fetch:1": _site(
        "ibkr.gap_fill.head_daily_probe", "ibkr-bars", "a1_context_required", _A1_SHARED),
    "engine.stock_ibkr:_probe_earliest_daily:adapter_fetch:2": _site(
        "ibkr.gap_fill.head_daily_probe", "ibkr-bars", "a1_context_required", _A1_SHARED),
    "engine.stock_ibkr:_probe_covered_daily_prefix:adapter_fetch:1": _site(
        "ibkr.gap_fill.head_daily_probe", "ibkr-bars", "a1_context_required",
        "Shared daily-probe coverage-edge decomposition under the same scoped producer; each carrier is paced and ledgered"),
    "engine.stock_ibkr:_retired_spot_check_day:adapter_fetch:1": _site(
        "ibkr.stock_ibkr.retired_spot_check", "ibkr-bars", "a2_refused", _A2_IBKR),
    "engine.stock_ibkr:_doctor_body:adapter_fetch:1": _site(
        "ibkr.stock_ibkr.doctor", "ibkr-bars", "a2_refused", _A2_IBKR),
    "engine.stock_ibkr:earliest_available:adapter_head:1": _site(
        "ibkr.earliest_available.head_timestamp", "ibkr-head", "a1_context_required", _A1_SHARED),
    "engine.stock_ibkr:earliest_available:adapter_head:2": _site(
        "ibkr.earliest_available.head_timestamp", "ibkr-head", "a1_context_required", _A1_SHARED),
    "engine.stock_ibkr:_probe_max_span_body:adapter_fetch:1": _site(
        "ibkr.stock_ibkr.span_probe", "ibkr-bars", "a2_refused", _A2_IBKR),
    "engine.stock_validate:fetch_daily_reference:http_urlopen:1": _site(
        "http.stockanalysis.validation", "http-series", "a2_refused", _A2_HTTP),
    "engine.two_account_pacing_probe:hammer:adapter_fetch:1": _site(
        "ibkr.two_account_pacing_probe.hammer", "ibkr-bars", "a2_refused", _A2_IBKR),
    "engine.tws_discovery:scan_listening_ports:socket:1": _site(
        "socket.tws_discovery.scan", "socket-control", "control_socket", _CONTROL_SOCKET),
    "engine.tws_launch:port_open:socket:1": _site(
        "socket.tws_launch.port_open", "socket-control", "control_socket", _CONTROL_SOCKET),
    "engine.vol_value_bank:fetch_ratio_day:adapter_fetch:1": _site(
        "ibkr.vol_value_bank.ratio_day", "ibkr-bars", "a2_refused", _A2_IBKR),
}


A1_CALL_PATHS: Mapping[str, tuple[str, ...]] = {
    "ibkr.earliest_available.head_timestamp": (
        "gap_fill_parallel", "gap_fill_parallel.worker", "gap_fill",
        "earliest_available",
    ),
    "ibkr.gap_fill.head_timestamp": (
        "gap_fill_parallel", "gap_fill_parallel.worker", "gap_fill",
        "_fill_series", "_fill_series_inner", "_backfill_earlier",
        "_head_start_evidence",
    ),
    "ibkr.gap_fill.session_fetch": (
        "gap_fill_parallel", "gap_fill_parallel.worker", "gap_fill",
        "_prefetch_combined", "_prefetch_unfiltered", "_fetch_request",
    ),
    "ibkr.gap_fill.head_daily_probe": (
        "gap_fill_parallel", "gap_fill_parallel.worker", "gap_fill",
        "_fill_series", "_fill_series_inner", "_backfill_earlier",
        "_head_start_evidence",
        "_probe_earliest_daily",
    ),
    "ibkr.gap_fill.month_daily": (
        "gap_fill_parallel", "gap_fill_parallel.worker", "gap_fill",
        "_fill_series", "_fill_series_inner", "_backfill_earlier",
        "_backfill_seal_interior",
        "fill_missing_days", "_fill_missing_days_body", "_fetch_month_bars",
    ),
    "ibkr.gap_fill.month_intraday": (
        "gap_fill_parallel", "gap_fill_parallel.worker", "gap_fill",
        "_fill_series", "_fill_series_inner", "_backfill_earlier",
        "_backfill_seal_interior",
        "fill_missing_days", "_fill_missing_days_body", "_fetch_month_bars",
    ),
}


def _dotted_name(node: ast.AST) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        base = _dotted_name(node.value)
        return f"{base}.{node.attr}" if base else None
    return None


def _resolved_dotted(node: ast.AST,
                     aliases: Mapping[str, str]) -> str | None:
    name = _dotted_name(node)
    if name is None:
        return None
    first, dot, rest = name.partition(".")
    replacement = aliases.get(first)
    if replacement is None:
        return name
    return replacement + (dot + rest if dot else "")


def _call_target(node: ast.AST, aliases: Mapping[str, str]) -> str | None:
    name = _resolved_dotted(node, aliases)
    if name is not None:
        return name
    if (isinstance(node, ast.Call)
            and _resolved_dotted(node.func, aliases) == "getattr"
            and len(node.args) >= 2
            and isinstance(node.args[1], ast.Constant)
            and isinstance(node.args[1].value, str)):
        base = _resolved_dotted(node.args[0], aliases)
        if base:
            return f"{base}.{node.args[1].value}"
        return str(node.args[1].value)
    return None


def _contains_dotted(node: ast.AST, target: str,
                     aliases: Mapping[str, str]) -> bool:
    if _resolved_dotted(node, aliases) == target:
        return True
    return any(_contains_dotted(child, target, aliases)
               for child in ast.iter_child_nodes(node))


def _primitive(call: ast.Call, aliases: Mapping[str, str]) -> str | None:
    name = _call_target(call.func, aliases)
    is_getattr_call = (isinstance(call.func, ast.Call)
                       and _resolved_dotted(call.func.func, aliases) == "getattr")
    method = (call.func.attr if isinstance(call.func, ast.Attribute)
              else name.rsplit(".", 1)[-1] if is_getattr_call and name else None)
    if method == "fetch":
        return "adapter_fetch"
    if method == "head_timestamp":
        return "adapter_head"
    if method == "reqHistoricalDataAsync":
        return "ibkr_raw_bars"
    if method == "reqHeadTimeStampAsync":
        return "ibkr_raw_head"
    if (method and method.startswith("req") and len(method) > 3
            and "A" <= method[3] <= "Z"):
        return "ibkr_raw_request"
    if method in IBKR_NON_REQ_METHODS:
        return "ibkr_non_req"
    if name in {"urllib.request.urlopen", "urlopen"}:
        return "http_urlopen"
    if name == "urllib.request.OpenerDirector.open":
        return "http_opener_open"
    if name == "socket.getaddrinfo":
        return "socket_dns"
    if (isinstance(call.func, ast.BoolOp)
            and _contains_dotted(call.func, "urllib.request.urlopen", aliases)):
        return "http_urlopen"
    if name and name.startswith("requests.") and name.rsplit(".", 1)[-1] in {
            "get", "post", "put", "delete", "head", "patch", "request"}:
        return "http_requests"
    if name and name.startswith("http.client."):
        return "http_client"
    if name in {"socket.socket", "socket.create_connection"}:
        return "socket"
    return None


class _CandidateVisitor(ast.NodeVisitor):
    def __init__(self, module: str, source: str):
        self.module = module
        self.source = source
        self.stack: list[str] = []
        self.aliases: dict[str, str] = {}
        self.candidates: list[dict[str, object]] = []

    def visit_Import(self, node: ast.Import) -> None:  # noqa: N802
        for alias in node.names:
            if alias.asname:
                self.aliases[alias.asname] = alias.name
        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:  # noqa: N802
        if node.module:
            for alias in node.names:
                if alias.name != "*":
                    self.aliases[alias.asname or alias.name] = (
                        f"{node.module}.{alias.name}")
        self.generic_visit(node)

    def visit_ClassDef(self, node: ast.ClassDef) -> None:  # noqa: N802
        self.stack.append(node.name)
        self.generic_visit(node)
        self.stack.pop()

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:  # noqa: N802
        self.stack.append(node.name)
        self.generic_visit(node)
        self.stack.pop()

    visit_AsyncFunctionDef = visit_FunctionDef

    def visit_Call(self, node: ast.Call) -> None:  # noqa: N802
        primitive = _primitive(node, self.aliases)
        if primitive is not None:
            qualname = ".".join(self.stack) if self.stack else "<module>"
            if (primitive == "adapter_fetch"
                    and self.module == "engine.external_sweep"
                    and qualname == "_provider_fetch"):
                primitive = "provider_fetch"
            segment = ast.get_source_segment(self.source, node)
            if segment is None:
                raise InventoryError(
                    f"{self.module}:{node.lineno}: AST call has no source segment")
            self.candidates.append({
                "module": self.module,
                "qualname": qualname,
                "caller_chain": list(self.stack),
                "primitive": primitive,
                "line": int(node.lineno),
                "column": int(node.col_offset),
                "call": " ".join(segment.split()),
            })
        self.generic_visit(node)


def scan_source(source: str, module: str) -> list[dict[str, object]]:
    try:
        tree = ast.parse(source, filename=module)
    except SyntaxError as exc:
        raise InventoryError(f"cannot parse {module}: {exc}") from exc
    visitor = _CandidateVisitor(module, source)
    visitor.visit(tree)
    candidates = sorted(
        visitor.candidates,
        key=lambda row: (int(row["line"]), int(row["column"]), str(row["primitive"])),
    )
    ordinals: Counter[tuple[str, str]] = Counter()
    for row in candidates:
        group = (str(row["qualname"]), str(row["primitive"]))
        ordinals[group] += 1
        row["site_key"] = (
            f"{module}:{row['qualname']}:{row['primitive']}:{ordinals[group]}"
        )
    return candidates


def _shipped_sources(engine_root: Path) -> Iterable[Path]:
    project_root = engine_root.parent
    paths = list(project_root.glob("*.py"))
    for directory in SHIPPED_SUBDIRS:
        root = engine_root if directory == "engine" else project_root / directory
        paths.extend(root.rglob("*.py"))
    for path in sorted(paths, key=lambda p: p.relative_to(project_root).as_posix()):
        if path.name in EXCLUDED_NAMES or path.name.endswith(EXCLUDED_SUFFIXES):
            continue
        yield path


def scan_tree(engine_root: Path = ENGINE_ROOT) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for path in _shipped_sources(engine_root):
        relative = path.relative_to(engine_root.parent).with_suffix("")
        parts = relative.parts
        # Preserve the public engine_root override for isolated test trees.
        if path.is_relative_to(engine_root):
            parts = ("engine", *path.relative_to(engine_root).with_suffix("").parts)
        module = ".".join(parts)
        try:
            source = path.read_text(encoding="utf-8")
        except (OSError, UnicodeError) as exc:
            raise InventoryError(f"cannot read {path}: {exc}") from exc
        rows.extend(scan_source(source, module))
    return sorted(rows, key=lambda row: str(row["site_key"]))


class _LocalCallGraph(ast.NodeVisitor):
    def __init__(self):
        self.stack: list[str] = []
        self.kinds: list[str] = []
        self.edges: set[tuple[str, str]] = set()
        self.calls: list[tuple[str, ast.AST]] = []
        # Union all lexical bindings, including nested imports. Shadowing may
        # over-refuse but cannot erase an earlier send-bearing import.
        self.imports: dict[str, set[str]] = defaultdict(set)

    def visit_Import(self, node: ast.Import) -> None:  # noqa: N802
        for alias in node.names:
            bound = alias.asname or alias.name.split(".")[0]
            self.imports[bound].add(alias.name if alias.asname else bound)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:  # noqa: N802
        prefix = "engine." if node.level == 1 else ""
        module = (prefix + (node.module or "")).rstrip(".")
        for alias in node.names:
            self.imports[alias.asname or alias.name].add(
                ".".join(part for part in (module, alias.name) if part))

    def visit_ClassDef(self, node: ast.ClassDef) -> None:  # noqa: N802
        self.stack.append(node.name)
        self.kinds.append("class")
        self.generic_visit(node)
        self.kinds.pop()
        self.stack.pop()

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:  # noqa: N802
        parent = ".".join(self.stack)
        parent_is_function = bool(self.kinds and self.kinds[-1] == "function")
        self.stack.append(node.name)
        self.kinds.append("function")
        current = ".".join(self.stack)
        if parent_is_function:
            self.edges.add((parent, current))
        self.generic_visit(node)
        self.kinds.pop()
        self.stack.pop()

    visit_AsyncFunctionDef = visit_FunctionDef

    def visit_Call(self, node: ast.Call) -> None:  # noqa: N802
        if self.stack:
            owner = ".".join(self.stack)
            self.calls.append((owner, node.func))
            if isinstance(node.func, ast.Name):
                self.edges.add((owner, node.func.id))
        self.generic_visit(node)


def _check_a1_exclusion(graph: _LocalCallGraph,
                        sites: list[dict[str, object]],
                        offline_helpers: frozenset[str] = frozenset()) -> None:
    """Conservative local Name-call/nested-function proof, not receiver inference.

    A class constructor is not a call to all of that class's methods.
    Resolved imports into send-bearing modules and named A2 methods have a
    conservative residual pin. Dynamic dispatch/callbacks still need guards.
    """
    successors: dict[str, set[str]] = defaultdict(set)
    for caller, callee in graph.edges:
        successors[caller].add(callee)
    reachable: set[str] = set()
    pending = ["gap_fill_parallel"]
    while pending:
        owner = pending.pop()
        if owner in reachable:
            continue
        reachable.add(owner)
        pending.extend(successors[owner] - reachable)
    forbidden = sorted(str(site["site_key"]) for site in sites
                       if site["module"] == "engine.stock_ibkr"
                       and site["disposition"] == "a2_refused"
                       and site["qualname"] in reachable)
    if forbidden:
        raise InventoryError(f"A2-refused owners reachable from nightly root: {forbidden}")
    _check_a1_residual(graph, sites, reachable, offline_helpers)


def _check_a1_residual(graph: _LocalCallGraph,
                       sites: list[dict[str, object]], reachable: set[str],
                       offline_helpers: frozenset[str] = frozenset()) -> None:
    """O1: keep the reviewed absence of attribute/import send routes pinned.

    No module is imported/executed to prove this. Bare imports of engine modules
    use the same canonical identity as package/relative imports. Any function
    in a send-bearing module is conservatively refused, except the two
    separately source-pinned sidecar helpers. No module-wide exemption.
    Computed names, assigned method references and callback targets are outside
    this lexical proof and must be caught by the production runtime guard.
    """
    modules = {str(site["module"]) for site in sites}
    module_names = modules | {name.removeprefix("engine.") for name in modules
                              if name.startswith("engine.")}
    forbidden = []
    for owner, function in graph.calls:
        if owner not in reachable:
            continue
        name = _call_target(function, {})
        method = (function.attr if isinstance(function, ast.Attribute)
                  else name.rsplit(".", 1)[-1] if name else None)
        if method in {"company_name", "search"}:
            forbidden.append(f"{owner}: A2 method {method}")
        if name:
            first, dot, rest = name.partition(".")
            targets = {name} | {bound + (dot + rest)
                                for bound in graph.imports.get(first, set())}
            for target in targets:
                if (target not in offline_helpers
                        and any(target.startswith(module + ".") for module in module_names)):
                    forbidden.append(f"{owner}: send-bearing module {target}")
        for wildcard in graph.imports.get("*", set()):
            if wildcard.removesuffix(".*") in module_names:
                forbidden.append(f"{owner}: send-bearing wildcard {wildcard}")
    if forbidden:
        raise InventoryError(f"A1 residual exclusion violated: {sorted(set(forbidden))}")


def _offline_helper_exceptions(engine_root: Path,
                                sites: list[dict[str, object]]) -> frozenset[str]:
    if not any(site["module"] == "engine.stock_validate" for site in sites):
        return frozenset()
    source = (engine_root / "stock_validate.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    for target, expected in A1_OFFLINE_HELPER_SHA256.items():
        nodes = [node for node in tree.body if isinstance(node, ast.FunctionDef)
                 and node.name == target.rsplit(".", 1)[-1]]
        if (len(nodes) != 1 or hashlib.sha256(
                ast.get_source_segment(source, nodes[0]).encode("utf-8")).hexdigest() != expected):
            raise InventoryError(f"A1 offline helper source drift: {target}")
    # The reviewed bodies call only each other, builtins, these imports, and
    # local-file methods. Resolve imports across the module conservatively:
    # changing/rebinding an imported dependency requires a new review too.
    graph = _LocalCallGraph()
    graph.visit(tree)
    for name, expected in {"json": "json", "Path": "pathlib.Path",
                           "ss": "stock_storage", "date": "datetime.date"}.items():
        if graph.imports.get(name) != {expected}:
            raise InventoryError(f"A1 offline helper import drift: {name}")
    return frozenset(A1_OFFLINE_HELPER_SHA256) | frozenset(
        name.removeprefix("engine.") for name in A1_OFFLINE_HELPER_SHA256)


def _a1_paths(engine_root: Path, sites: list[dict[str, object]]) -> None:
    a1_sites = [site for site in sites
                if site["disposition"] == "a1_context_required"]
    source_path = engine_root / "stock_ibkr.py"
    if not a1_sites and not source_path.is_file():
        return
    source = source_path.read_text(encoding="utf-8")
    graph = _LocalCallGraph()
    graph.visit(ast.parse(source, filename="engine.stock_ibkr"))
    _check_a1_exclusion(graph, sites, _offline_helper_exceptions(engine_root, sites))
    if not a1_sites:
        return
    actual_producers = {str(site["producer_id"]) for site in a1_sites}
    if actual_producers != set(A1_CALL_PATHS):
        raise InventoryError(
            "A1 producer/path mismatch: "
            f"sites={sorted(actual_producers)} paths={sorted(A1_CALL_PATHS)}")
    for site in a1_sites:
        producer = str(site["producer_id"])
        path = A1_CALL_PATHS[producer]
        if site["site_key"] == "engine.stock_ibkr:_probe_covered_daily_prefix:adapter_fetch:1":
            path = path + ("_probe_covered_daily_prefix",)
        if path[0] != "gap_fill_parallel" or path[-1] != site["qualname"]:
            raise InventoryError(f"{producer}: invalid A1 path endpoints {path}")
        missing = [edge for edge in zip(path, path[1:]) if edge not in graph.edges]
        if missing:
            raise InventoryError(f"{producer}: unproven A1 call path edges {missing}")
        site["a1_call_path"] = list(path)


def _candidate_surface_digest(candidates: list[dict[str, object]]) -> str:
    payload = json.dumps(
        candidates, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def build_inventory(
        engine_root: Path = ENGINE_ROOT,
        classifications: Mapping[str, Mapping[str, object]] = SITE_CLASSIFICATIONS,
) -> dict[str, object]:
    candidates = scan_tree(engine_root)
    keys = [str(row["site_key"]) for row in candidates]
    if len(keys) != len(set(keys)):
        duplicates = sorted(key for key, count in Counter(keys).items() if count != 1)
        raise InventoryError(f"duplicate site keys: {duplicates}")
    missing = sorted(set(keys) - set(classifications))
    stale = sorted(set(classifications) - set(keys))
    if missing or stale:
        raise InventoryError(
            f"classification mismatch: unclassified={missing}; stale={stale}")

    sites: list[dict[str, object]] = []
    for candidate in candidates:
        key = str(candidate["site_key"])
        classification = dict(classifications[key])
        required = {"producer_id", "transport", "disposition", "reason"}
        absent = sorted(required - set(classification))
        extra = sorted(set(classification) - required)
        if absent or extra:
            raise InventoryError(
                f"{key}: classification fields absent={absent} extra={extra}")
        if not all(isinstance(classification[field], str) and classification[field]
                   for field in required):
            raise InventoryError(f"{key}: classification fields must be non-empty strings")
        sites.append({**candidate, **classification})

    _a1_paths(engine_root, sites)

    producer_counts = Counter(str(site["producer_id"]) for site in sites)
    raw_bars = [site for site in sites if site["primitive"] == "ibkr_raw_bars"]
    raw_heads = [site for site in sites if site["primitive"] == "ibkr_raw_head"]
    if len(raw_bars) != 1 or len(raw_heads) != 1:
        raise InventoryError(
            "raw IBKR choke-point mismatch: "
            f"bars={len(raw_bars)} heads={len(raw_heads)}")
    return {
        "schema_version": SCHEMA_VERSION,
        "candidate_surface_sha256": _candidate_surface_digest(candidates),
        "candidate_count": len(sites),
        "logical_producer_count": len(producer_counts),
        "primitive_counts": dict(sorted(Counter(
            str(site["primitive"]) for site in sites).items())),
        "disposition_counts": dict(sorted(Counter(
            str(site["disposition"]) for site in sites).items())),
        "raw_choke_points": {
            "ibkr_bars": raw_bars[0]["site_key"],
            "ibkr_head": raw_heads[0]["site_key"],
        },
        "a1_offline_helper_sha256": dict(A1_OFFLINE_HELPER_SHA256),
        "sites": sites,
    }


def render_inventory(inventory: Mapping[str, object]) -> bytes:
    return (json.dumps(inventory, indent=2, sort_keys=True) + "\n").encode("utf-8")


def _atomic_write(path: Path, payload: bytes) -> None:
    tmp = path.with_name(f".{path.name}.tmp")
    try:
        with open(tmp, "wb") as handle:
            handle.write(payload)
            handle.flush()
        tmp.replace(path)
    finally:
        try:
            tmp.unlink()
        except FileNotFoundError:
            pass


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument("--candidates", action="store_true")
    modes.add_argument("--check", action="store_true")
    modes.add_argument("--write", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    try:
        if args.candidates:
            for row in scan_tree():
                print(f"{row['site_key']}\tline={row['line']}\t{row['call']}")
            return 0
        payload = render_inventory(build_inventory())
        if args.write:
            _atomic_write(INVENTORY_PATH, payload)
            print(f"wrote {INVENTORY_PATH.name} ({len(payload)} bytes)")
            return 0
        if not INVENTORY_PATH.is_file():
            raise InventoryError(f"committed inventory missing: {INVENTORY_PATH}")
        current = INVENTORY_PATH.read_bytes()
        if current != payload:
            raise InventoryError("committed inventory differs from deterministic regeneration")
        print(f"inventory clean: {len(build_inventory()['sites'])} sites")
        return 0
    except (InventoryError, OSError, UnicodeError) as exc:
        print(f"FETCH SEND INVENTORY ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
