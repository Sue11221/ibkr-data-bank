"""Confined offline-only policy capability, never a production activation API.

Static caller inventory, exact registered suite ownership, live transport guards
and per-use expiry checks are complementary controls. This is not a sandbox for
malicious Python that can rewrite its own process; shipped callers are reviewed.
"""

import ast
from contextlib import contextmanager, ExitStack
import contextlib
import http.client
import inspect
import os
from pathlib import Path
import socket
import subprocess
import sys
import threading
from types import MappingProxyType
import urllib.request
from unittest.mock import patch

from fetch_authority import AuthorityError


OFFLINE_SUITES = frozenset({"fetch_a2_infrastructure_selftest", "fetch_a2_ibkr_workflows_selftest",
                            "fetch_a2_http_workflows_selftest"})
_ROOT = Path(__file__).resolve().parents[1]
_FILE = Path(__file__).resolve()
_CONTEXTLIB = Path(contextlib.__file__).resolve()
_API = frozenset({"offline_policy", "offline_transports", "_Policy", "_OfflineTransportGuard"})
_LOCK = threading.RLock()
_ACTIVE = None
_AUDIT_INSTALLED = False

# Existing A1 sends need the confined capability under non-nightly purposes.
# Keep them distinct from future endpoints: legacy/nightly A1 default admission
# is intentional, whereas every TEST_PRODUCERS endpoint remains default-held.
TEST_SHARED_PRODUCERS = MappingProxyType({
    "ibkr.gap_fill.month_daily": ("ibkr-bars", None),
    "ibkr.gap_fill.month_intraday": ("ibkr-bars", None),
    "ibkr.gap_fill.session_fetch": ("ibkr-bars", None),
    "ibkr.gap_fill.head_daily_probe": ("ibkr-bars", None),
    "ibkr.gap_fill.head_timestamp": ("ibkr-head", None),
    "ibkr.earliest_available.head_timestamp": ("ibkr-head", None),
    "ibkr.choke.qualify": ("ibkr-metadata", None),
    "ibkr.choke.qualify_many": ("ibkr-metadata", None),
})

# Finite future support, not production permission. Endpoint IDs are mandatory
# for HTTP so the shared split sender cannot become an unrestricted URL opener.
TEST_PRODUCERS = MappingProxyType({
    "http.stockanalysis.validation": ("http-series", "stockanalysis.history"),
    "http.stockanalysis.external_sweep": ("http-series", "stockanalysis.history"),
    "http.split_provider.yahoo": ("http-series", "yahoo.splits"),
    "http.split_provider.sec_tickers": ("http-metadata", "sec.tickers"),
    "http.split_provider.sec_facts": ("http-metadata", "sec.companyfacts"),
    "http.split_provider.issuer": ("http-metadata", "fortinet.history"),
    "http.sp500.github": ("http-metadata", "sp500.github"),
    "http.sp500.wikipedia": ("http-metadata", "sp500.wikipedia"),
    "ibkr.metadata.company_name": ("ibkr-metadata", None),
    "ibkr.metadata.symbol_search": ("ibkr-metadata", None),
    "ibkr.identity.head_timestamp": ("ibkr-head", None),
    "ibkr.stock_ibkr.doctor": ("ibkr-bars", None),
    "ibkr.stock_ibkr.span_probe": ("ibkr-bars", None),
    "ibkr.stock_ibkr.retired_spot_check": ("ibkr-bars", None),
    "ibkr.live_spot_probe.daily": ("ibkr-bars", None),
    "ibkr.live_spot_probe.minute": ("ibkr-bars", None),
    "ibkr.vol_value_bank.ratio_day": ("ibkr-bars", None),
    "ibkr.live_internal_revalidate.daily": ("ibkr-bars", None),
    "ibkr.live_internal_revalidate.minute_refetch": ("ibkr-bars", None),
    "ibkr.live_internal_daily_check.daily": ("ibkr-bars", None),
    "ibkr.live_combined_flags.minute_month": ("ibkr-bars", None),
    "ibkr.live_combined_flags.daily_refetch": ("ibkr-bars", None),
    "ibkr.live_kind_smoke.iv_rth": ("ibkr-bars-unfiltered", None),
    "ibkr.live_kind_smoke.iv_daily": ("ibkr-bars", None),
    "ibkr.live_kind_smoke.iv_intraday": ("ibkr-bars", None),
    "ibkr.live_kind_smoke.hvol_daily": ("ibkr-bars", None),
    "ibkr.live_kind_smoke.hvol_intraday": ("ibkr-bars", None),
    "ibkr.live_kind_smoke.span_probe": ("ibkr-bars", None),
    "ibkr.two_account_pacing_probe.hammer": ("ibkr-bars", None),
})


def _registered_owner(path):
    if path.parent != _ROOT / "engine" or path.stem not in OFFLINE_SUITES:
        return False
    import run_gates
    return any(spec.name == path.stem and Path(spec.script).name == path.name
               for spec in run_gates.BATTERIES["all"])


def _check_caller():
    for frame in inspect.stack(context=0)[1:]:
        path = Path(frame.filename).resolve()
        if path in {_FILE, _CONTEXTLIB}:
            continue
        if (not _registered_owner(path)
                or Path(frame.frame.f_globals.get("__file__", "")).resolve() != path):
            raise AuthorityError("offline policy API caller is not a registered suite")
        return
    raise AuthorityError("offline policy API has no registered caller")


def _deny(*args, **kwargs):
    raise AuthorityError("offline transport tripwire")


def _audit(event, args):
    if _ACTIVE is not None and (event.startswith("socket.") or event in {
            "subprocess.Popen", "os.system", "os.startfile", "os.startfile/2", "os.posix_spawn"}):
        _deny()


def _guard_targets():
    targets = [(socket, "create_connection"), (socket, "getaddrinfo"),
        (socket.socket, "connect"), (socket.socket, "connect_ex"),
        (socket.socket, "sendto"), (urllib.request, "urlopen"),
        (http.client.HTTPConnection, "connect"),
        (http.client.HTTPSConnection, "connect"), (subprocess, "Popen"), (os, "system")]
    if hasattr(os, "startfile"):
        targets.append((os, "startfile"))
    return targets


class _OfflineTransportGuard:
    __slots__ = ("targets", "live")

    def __init__(self, targets):
        _check_caller()
        self.targets = tuple(targets)
        self.live = True

    def verify(self):
        if (not self.live or _ACTIVE is not self
                or any(getattr(owner, name) is not _deny for owner, name in self.targets)):
            raise AuthorityError("offline policy requires intact live transport tripwires")


@contextmanager
def offline_transports():
    _check_caller()
    global _ACTIVE, _AUDIT_INSTALLED
    with _LOCK:
        if _ACTIVE is not None:
            raise AuthorityError("offline transport guard cannot overlap")
        targets = _guard_targets()
        with ExitStack() as stack:
            for owner, name in targets:
                stack.enter_context(patch.object(owner, name, _deny))
            guard = _OfflineTransportGuard(targets)
            if not _AUDIT_INSTALLED:
                sys.addaudithook(_audit)
                _AUDIT_INSTALLED = True
            _ACTIVE = guard
            try:
                yield
            finally:
                guard.live = False
                _ACTIVE = None


class _Policy:
    __slots__ = ("_guard", "_live")

    def __init__(self, guard):
        _check_caller()
        _require_guard(guard)
        self._guard = guard
        self._live = True

    def allows(self, producer, variant, endpoint):
        _require_guard(self._guard)
        if not getattr(self, "_live", False):
            raise AuthorityError("offline policy capability expired")
        return (TEST_PRODUCERS.get(producer) == (variant, endpoint)
                or TEST_SHARED_PRODUCERS.get(producer) == (variant, endpoint))


def _require_guard(guard):
    if type(guard) is not _OfflineTransportGuard or guard is not _ACTIVE:
        raise AuthorityError("offline policy requires intact live transport tripwires")
    _OfflineTransportGuard.verify(guard)


def is_live_capability(capability):
    """Non-minting consumer check; never trust subclasses or duck-typed guards."""
    return (type(capability) is _Policy
            and type(getattr(capability, "_guard", None)) is _OfflineTransportGuard
            and capability._guard is _ACTIVE)


def capability_allows(capability, producer, variant, endpoint):
    if not is_live_capability(capability):
        raise AuthorityError("invalid test-policy capability: requires live tripwires")
    return _Policy.allows(capability, producer, variant, endpoint)


@contextmanager
def offline_policy():
    _check_caller()
    if _ACTIVE is None:
        raise AuthorityError("offline policy requires a transport tripwire")
    _require_guard(_ACTIVE)
    policy = _Policy(_ACTIVE)
    try:
        yield policy
    finally:
        policy._live = False


def check_callers(sources=None):
    """Static API/import/reference inventory; synthetic sources prove inverses.

    Aliasing a protected imported symbol is still visible at its import. Any
    protected name or constant reference outside exact offline owners fails.
    This conservative gate rejects references, not only immediate calls.
    """
    if sources is None:
        paths = list(_ROOT.glob("*.py"))
        for directory in ("engine", "tools", "ops"):
            paths.extend((_ROOT / directory).rglob("*.py"))
        sources = {path.relative_to(_ROOT).as_posix(): path.read_text(encoding="utf-8-sig")
                   for path in paths}
    owners = set()
    for relative, source in sources.items():
        path = (_ROOT / relative).resolve()
        if path == _FILE:
            continue
        try:
            tree = ast.parse(source)
        except SyntaxError as exc:
            raise AuthorityError("cannot parse test-policy caller inventory") from exc
        references = any(
            (isinstance(node, ast.Name) and node.id in _API)
            or (isinstance(node, ast.Attribute) and node.attr in _API)
            or (isinstance(node, ast.alias) and node.name in _API)
            or (isinstance(node, ast.Constant) and isinstance(node.value, str)
                and node.value in _API)
            for node in ast.walk(tree))
        if references:
            if not _registered_owner(path):
                raise AuthorityError("production caller references offline policy API: " + relative)
            owners.add(path.stem)
    if sources is not None and not owners.issubset(OFFLINE_SUITES):
        raise AuthorityError("unknown test-policy owner")
    return sorted(owners)
