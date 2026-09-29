"""Explicit operation ownership for registered nightly and held A2 roots.

Opaque handles are registered by identity, never trusted by attributes. Bound
contexts require a live, thread-owned child on every request. Legacy standalone
contexts remain usable by low-level offline fixtures, never by shared fill roots.
"""

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
import inspect
from pathlib import Path
import threading
from types import MappingProxyType
import weakref

from fetch_authority import AuthorityError


_ROOT = Path(__file__).resolve().parents[1]
_FILE = Path(__file__).resolve()
CORE_SUITE = "fetch_a2_ibkr_workflows_selftest"
PRODUCTION_ROOTS = MappingProxyType({
    ("engine/stock_ibkr.py", "nightly_gap_fill"): "nightly",
    ("engine/stock_ibkr.py", "nightly_gap_fill_parallel"): "nightly",
    ("engine/stock_ibkr.py", "nightly_gap_fill_parallel_resilient"): "nightly",
    ("engine/stock_ibkr.py", "nightly_update"): "nightly",
    ("engine/stock_ibkr.py", "add_stocks_gap_fill"): "add_stocks",
    ("engine/stock_ibkr.py", "add_stocks_gap_fill_parallel_resilient"): "add_stocks",
    ("engine/stock_ibkr.py", "find_symbol"): "identity",
    ("engine/stock_ibkr.py", "company_lookup"): "identity",
    ("engine/stock_ibkr.py", "validate_symbols"): "identity",
    ("engine/stock_ibkr.py", "doctor"): "diagnostic",
    ("engine/stock_ibkr.py", "estimate_backfill"): "diagnostic",
    ("engine/stock_ibkr.py", "probe_max_span"): "diagnostic",
    ("engine/stock_ibkr.py", "fill_missing_days"): "repair",
    ("engine/live_spot_probe.py", "run_live_probe"): "diagnostic",
    ("engine/fix_data_pipeline.py", "run"): "repair",
    ("engine/live_internal_revalidate.py", "run_internal_revalidate"): "diagnostic",
    ("engine/live_pipeline_check.py", "run_pipeline_check"): "diagnostic",
    ("engine/live_internal_daily_check.py", "run_internal_daily_check"): "diagnostic",
    ("engine/live_feature_check.py", "run_feature_check"): "diagnostic",
    ("engine/live_combined_flags.py", "run_combined_flags"): "diagnostic",
    ("engine/live_kind_smoke.py", "run_kind_smoke"): "diagnostic",
    ("engine/two_account_pacing_probe.py", "run_pacing_probe"): "diagnostic",
    ("engine/live_run_all.py", "run_all"): "diagnostic",
    ("engine/live_validate_catchup.py", "run_catchup"): "diagnostic",
    ("engine/stock_validate.py", "validate_series"): "validation",
    ("engine/stock_validate.py", "cross_validate_ticker"): "validation",
    ("engine/stock_validate.py", "revalidate_library"): "validation",
    ("engine/stock_validate.py", "fetch_daily_reference_single_request"): "validation",
    ("engine/external_sweep.py", "sweep_bank"): "external_sweep",
    ("engine/external_sweep.py", "sweep_ticker"): "external_sweep",
})
_SHARED = frozenset({
    "ibkr.gap_fill.month_daily", "ibkr.gap_fill.month_intraday",
    "ibkr.gap_fill.session_fetch", "ibkr.gap_fill.head_daily_probe",
    "ibkr.gap_fill.head_timestamp", "ibkr.earliest_available.head_timestamp",
    "ibkr.choke.qualify", "ibkr.choke.qualify_many",
})
PURPOSE_PRODUCERS = MappingProxyType({
    "nightly": _SHARED,
    "add_stocks": _SHARED | {"ibkr.vol_value_bank.ratio_day",
                             "ibkr.live_spot_probe.daily", "ibkr.live_spot_probe.minute"},
    "repair": _SHARED | {"ibkr.vol_value_bank.ratio_day",
                         "ibkr.live_spot_probe.daily", "ibkr.live_spot_probe.minute",
                         "ibkr.live_combined_flags.minute_month",
                         "ibkr.live_combined_flags.daily_refetch",
                         "http.stockanalysis.validation"},
    "diagnostic": _SHARED | {"ibkr.stock_ibkr.doctor", "ibkr.stock_ibkr.span_probe",
                             "http.stockanalysis.validation",
                             "ibkr.live_internal_revalidate.daily",
                             "ibkr.live_internal_revalidate.minute_refetch",
                             "ibkr.live_internal_daily_check.daily",
                             "ibkr.live_combined_flags.minute_month",
                             "ibkr.live_combined_flags.daily_refetch",
                             "ibkr.live_kind_smoke.iv_daily",
                             "ibkr.live_kind_smoke.iv_intraday",
                             "ibkr.live_kind_smoke.hvol_daily",
                             "ibkr.live_kind_smoke.hvol_intraday",
                             "ibkr.live_kind_smoke.iv_rth",
                             "ibkr.live_kind_smoke.span_probe",
                             "ibkr.two_account_pacing_probe.hammer",
                             "ibkr.stock_ibkr.retired_spot_check",
                             "ibkr.live_spot_probe.daily", "ibkr.live_spot_probe.minute"},
    "identity": frozenset({"ibkr.choke.qualify", "ibkr.choke.qualify_many",
        "ibkr.metadata.company_name", "ibkr.metadata.symbol_search",
        "ibkr.identity.head_timestamp", "ibkr.gap_fill.head_daily_probe"}),
    "validation": frozenset({"http.stockanalysis.validation"}),
    "external_sweep": frozenset({"http.stockanalysis.external_sweep"}),
})
_LOCK = threading.RLock()
_OPERATIONS, _CHILDREN, _CONTEXTS, _LEDGERS = {}, {}, {}, {}
_CURRENT = ContextVar("fetch_operation_child", default=None)
_OBSERVER = ContextVar("fetch_operation_observer", default=0)
_SEALING = ContextVar("fetch_operation_sealing", default=None)
_CLOSING = ContextVar("fetch_operation_closing", default=None)


def _refuse(message):
    raise AuthorityError(message)


def _not_observer():
    if _OBSERVER.get():
        _refuse("observer has no operation authority")


def _test_capability(capability):
    from fetch_test_policy import capability_allows
    if not capability_allows(capability, "ibkr.metadata.symbol_search", "ibkr-metadata", None):
        _refuse("operation core requires its confined test capability")


def _root_owner(purpose):
    frame = inspect.currentframe().f_back.f_back
    try:
        path = Path(frame.f_code.co_filename).resolve()
        if Path(frame.f_globals.get("__file__", "")).resolve() != path:
            _refuse("operation mint caller is not a registered root")
        relative = path.relative_to(_ROOT).as_posix() if path.is_relative_to(_ROOT) else ""
        key = (relative, frame.f_code.co_qualname)
        if key in PRODUCTION_ROOTS:
            if PRODUCTION_ROOTS[key] != purpose:
                _refuse("registered root purpose changed")
            return False
        import run_gates
        if (path != _ROOT / "engine" / (CORE_SUITE + ".py")
                or frame.f_code.co_qualname != "Operations.open"
                or not any(s.name == CORE_SUITE for s in run_gates.BATTERIES["all"])):
            _refuse("operation mint caller is not a registered root")
        return True
    finally:
        del frame


@dataclass
class _Run:
    context: object
    purpose: str
    capability: object
    test_only: bool = False
    children: set = field(default_factory=set)
    sealed: bool = False
    sealing: bool = False


@dataclass
class _Child:
    owner: object
    worker_id: str
    rights: frozenset
    parent: object = None
    children: set = field(default_factory=set)
    thread: int | None = None
    active: bool = False
    attempts: int = 0


def _run(handle):
    _not_observer()
    if type(handle) is not _OperationHandle or handle not in _OPERATIONS:
        _refuse("invalid or expired operation handle")
    value = _OPERATIONS[handle]
    if value.test_only or value.capability is not None:
        _test_capability(value.capability)
    return value


def _child(handle):
    _not_observer()
    if type(handle) is not _ChildHandle or handle not in _CHILDREN:
        _refuse("invalid or expired child handle")
    value = _CHILDREN[handle]
    _run(value.owner)
    return value


def _make_child(owner, worker_id, rights, parent=None):
    from fetch_ledger import identity
    run = _run(owner)
    if run.sealed or run.sealing:
        _refuse("sealed operation cannot create children")
    allowed = PURPOSE_PRODUCERS[run.purpose] if parent is None else _child(parent).rights
    requested = allowed if rights is None else frozenset(rights)
    if not requested.issubset(allowed):
        _refuse("child cannot widen producer rights")
    worker_id = identity(worker_id)
    # Reusing a worker ID would weaken account-rebinding and sibling isolation.
    if any(_CHILDREN[c].worker_id == worker_id for c in run.children):
        _refuse("worker ID already belongs to a live child")
    handle = object.__new__(_ChildHandle)
    _CHILDREN[handle] = _Child(owner, worker_id, requested, parent)
    run.children.add(handle)
    if parent is not None:
        _CHILDREN[parent].children.add(handle)
    return handle


class _OperationHandle:
    __slots__ = ()

    def __new__(cls, *args, **kwargs):
        _refuse("operation handles require a registered root")

    @property
    def context(self):
        with _LOCK:
            return _run(self).context

    @property
    def purpose(self):
        with _LOCK:
            return _run(self).purpose

    def child(self, worker_id, *, rights=None):
        with _LOCK:
            return _make_child(self, worker_id, rights)

    def seal(self):
        with _LOCK:
            run = _run(self)
            if run.children:
                _refuse("cannot seal while children are outstanding")
            if run.sealing:
                _refuse("operation seal is already in progress")
            run.sealing = True
        # Ledger fsync must not stall unrelated operations on the registry lock.
        # The state flag prevents child/close/second-seal races during this I/O.
        token = _SEALING.set(self)
        try:
            receipt = run.context.seal()
            with _LOCK:
                run.sealed = True
            return receipt
        finally:
            _SEALING.reset(token)
            with _LOCK:
                run.sealing = False

    def close(self):
        # Cleanup must remain possible after the test guard expires, but no
        # request, new child or seal can then be authorized.
        _not_observer()
        with _LOCK:
            if type(self) is not _OperationHandle or self not in _OPERATIONS:
                _refuse("invalid or expired operation handle")
            run = _OPERATIONS[self]
            if run.sealing:
                _refuse("cannot close while operation seal is in progress")
            if run.children:
                _refuse("cannot close while children are outstanding")
            token = _CLOSING.set(self)
            try:
                run.context.ledger.close()
            finally:
                _CLOSING.reset(token)
                # A failed stream close is terminal too. Keep the exception
                # visible, but never retain authority for a closed/failed sink.
                reference, _ = _CONTEXTS[id(run.context)]
                _CONTEXTS[id(run.context)] = (reference, None)  # Tombstone until context GC.
                del _OPERATIONS[self]


class _ChildHandle:
    __slots__ = ()

    def __new__(cls, *args, **kwargs):
        _refuse("child handles require an active parent")

    def child(self, worker_id, *, rights=None):
        with _LOCK:
            state = _child(self)
            _active_child(_OPERATIONS[state.owner].context, state.worker_id)
            return _make_child(state.owner, worker_id, rights, self)

    @contextmanager
    def scope(self):
        with _LOCK:
            state = _child(self)
            if state.active:
                _refuse("child is already active on a worker thread")
            state.active, state.thread = True, threading.get_ident()
        token = _CURRENT.set(self)
        try:
            yield self
        finally:
            _CURRENT.reset(token)
            with _LOCK:
                state.active, state.thread = False, None

    def worker(self):
        with _LOCK:
            state = _child(self)
            context = _OPERATIONS[state.owner].context
            _active_child(context, state.worker_id)
            return context.worker(state.worker_id)

    def close(self):
        _not_observer()
        with _LOCK:
            if type(self) is not _ChildHandle or self not in _CHILDREN:
                _refuse("invalid or expired child handle")
            state = _CHILDREN[self]
            if state.active or state.attempts or state.children:
                _refuse("child must join its scope, attempts and children before close")
            _OPERATIONS[state.owner].children.remove(self)
            if state.parent is not None:
                _CHILDREN[state.parent].children.remove(self)
            del _CHILDREN[self]


def begin_operation(purpose, directory, *, test_capability=None, **context_options):
    """Exact root admission; implemented A2 purposes remain send-disabled."""
    _not_observer()
    test_only = _root_owner(purpose)
    if test_only or test_capability is not None:
        _test_capability(test_capability)
    if type(purpose) is not str or purpose not in PURPOSE_PRODUCERS:
        _refuse("missing or unknown operation purpose")
    # StockAnalysis's ordinary GUI/CLI callers must refuse before authority,
    # clock or ledger effects. Older held IBKR roots retain their established
    # unverified-ledger refusal/report contract until separately reviewed.
    if purpose in {"validation", "external_sweep"} and test_capability is None:
        _refuse("operation purpose is not activated for production")
    from fetch_run_context import FetchRunContext
    if test_capability is not None:
        context_options = dict(context_options, test_capability=test_capability)
    context = FetchRunContext.create(directory, **context_options)
    with _LOCK:
        handle = object.__new__(_OperationHandle)
        _OPERATIONS[handle] = _Run(context, purpose, test_capability, test_only)
        key = id(context)
        _CONTEXTS[key] = (weakref.ref(context, lambda ref: _forget_context(key, ref)), handle)
        ledger_key = id(context.ledger)
        _LEDGERS[ledger_key] = (weakref.ref(context.ledger,
            lambda ref: _forget_ledger(ledger_key, ref)), weakref.ref(context))
    return handle


def require_root_admission(handle):
    """Before connection: no A2 workflow is production-enabled by its mint."""
    with _LOCK:
        run = _run(handle)
        if run.purpose != "nightly" and run.capability is None:
            _refuse("operation purpose is not activated for production")
        return run.context


def parent_context(handle):
    with _LOCK:
        return _run(handle).context


def narrow_worker_child(worker, worker_id, *, rights=None):
    """Transfer a nested task from this exact already-active worker, never mint."""
    from fetch_run_context import FetchWorker
    with _LOCK:
        if type(worker) is not FetchWorker:
            _refuse("nested task requires an explicit operation child")
        value = _active_child(worker.context, worker.worker_id)
        if value is None:
            _refuse("nested task requires an explicit operation child")
        parent = _CURRENT.get()
        return _make_child(_CHILDREN[parent].owner, worker_id, rights, parent)


@contextmanager
def scoped_worker(child, *, close=False):
    with _LOCK:
        _child(child)  # Exact registered handle before invoking any method.
    entered = False
    try:
        with child.scope():
            entered = True
            yield child.worker()
    finally:
        # A refused scope transfer does not own an already-active child.
        if close and entered:
            child.close()


def _forget_context(key, reference):
    with _LOCK:
        if _CONTEXTS.get(key, (None,))[0] is reference:
            del _CONTEXTS[key]


def _forget_ledger(key, reference):
    with _LOCK:
        if _LEDGERS.get(key, (None,))[0] is reference:
            del _LEDGERS[key]


def _ledger_context(ledger):
    entry = _LEDGERS.get(id(ledger))
    if entry is None or entry[0]() is not ledger:
        return None
    context = entry[1]()
    if context is None:
        _refuse("bound ledger context expired")
    return context


def _binding(context):
    canonical = _ledger_context(context.ledger)
    if canonical is not None and canonical is not context:
        _refuse("bound ledger cannot be adopted by a copied context")
    entry = _CONTEXTS.get(id(context))
    if entry is None or entry[0]() is not context:
        return None  # Genuine standalone legacy context, not a shared-root child.
    if entry[1] is None:
        _refuse("bound operation context expired")
    return entry[1]


def _active_child(context, worker_id=None):
    owner = _binding(context)
    if owner is None:
        return None
    run = _run(owner)
    child = _CURRENT.get()
    state = _child(child)
    if (state.owner is not owner or not state.active
            or state.thread != threading.get_ident()
            or (worker_id is not None and worker_id != state.worker_id)):
        _refuse("request lacks its explicit thread-owned child")
    return run, state


def check_worker(context, worker_id):
    with _LOCK:
        _active_child(context, worker_id)


def check_request(context, producer):
    with _LOCK:
        value = _active_child(context)
        if value is not None and producer not in value[1].rights:
            _refuse("producer is outside this operation purpose or child rights")


def operation_purpose(context):
    """Return registered purpose; None means an actual unbound legacy context."""
    with _LOCK:
        owner = _binding(context)
        return None if owner is None else _run(owner).purpose


@contextmanager
def request_scope(worker):
    with _LOCK:
        value = _active_child(worker.context, worker.worker_id)
        state = value[1] if value is not None else None
        if state is not None:
            state.attempts += 1
    try:
        yield
    finally:
        if state is not None:
            with _LOCK:
                state.attempts -= 1


def check_seal(context):
    with _LOCK:
        owner = _binding(context)
        if owner is not None:
            run = _run(owner)
            if _SEALING.get() is not owner or run.children:
                _refuse("only the joined parent may seal its context")


def check_ledger_lifecycle(ledger, *, closing=False):
    with _LOCK:
        context = _ledger_context(ledger)
        if context is None:
            return  # Legacy A1 ledger.
        owner = _binding(context)
        _not_observer()
        token = _CLOSING.get() if closing else _SEALING.get()
        if token is not owner or _OPERATIONS[owner].children:
            _refuse("only the joined parent may close or seal its ledger")


@contextmanager
def observer_scope():
    token = _OBSERVER.set(_OBSERVER.get() + 1)
    child = _CURRENT.set(None)
    try:
        yield
    finally:
        _CURRENT.reset(child)
        _OBSERVER.reset(token)
