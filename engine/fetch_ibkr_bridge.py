"""A1 request-scoped IB bridge. No context is retained by an adapter.

The producer installs a single-use capability only for its adapter invocation.
The live choke consumes it before any callback, pacing or transport work. Worker
scope alone is NOT permission to send; unrelated callbacks cannot borrow it.
"""

from contextlib import contextmanager, ExitStack, nullcontext
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import date, datetime, time
from functools import wraps
import inspect
import math
from types import SimpleNamespace

from fetch_authority import AuthorityError, BAR_SIZES, KINDS, NY, aware_ny, parse_token
from fetch_envelopes import carrier_start
from fetch_ledger import LedgerError
from fetch_run_context import FetchWorker, RequestRefused, RequestCancelled


_WORKER = ContextVar("a1_worker", default=None)
_SYMBOL = ContextVar("a1_symbol", default=None)
_SEND = ContextVar("a1_send", default=None)
_METADATA = ContextVar("a1_metadata", default=None)
_CANCEL = ContextVar("a1_cancel", default=None)
_PACER = ContextVar("a1_pacer", default=None)
_GOVERNOR = ContextVar("fetch_account_governor", default=None)
_REQUEST_ERROR_OBSERVER = ContextVar("fetch_request_error_observer", default=None)


def current_worker():
    return _WORKER.get()


def refuse_transferred_child(child, reason):
    """A named argument refusal owns only the valid inactive child transferred."""
    from fetch_operations import scoped_worker
    with scoped_worker(child, close=True):
        raise RequestRefused(reason)


def worker_scope(func):
    signature = inspect.signature(func)
    @wraps(func)
    def run(*args, **kwargs):
        worker = kwargs.pop("_fetch_worker", None)
        child = kwargs.pop("_fetch_child", None)
        with ExitStack() as stack:
            if child is not None:
                from fetch_operations import scoped_worker
                # Passing _fetch_child transfers its lifetime to this dispatch.
                # Enter its cleanup scope BEFORE binding/argument refusal, but
                # let the core validate the exact handle before invoking it.
                child_worker = stack.enter_context(scoped_worker(child, close=True))
                if worker is not None:
                    raise RequestRefused("child and legacy worker cannot be combined")
                worker = child_worker
            if func.__name__ in {"gap_fill", "_validate_symbols_body",
                    "_find_symbol_body", "_company_lookup_body", "_doctor_body",
                    "_estimate_backfill_body", "_probe_max_span_body",
                    "_fill_missing_days_body", "_retired_spot_check_day",
                    "_retired_run_spot_checks", "fetch_day", "fetch_span",
                    "_run_live_probe_body", "_run_embedded_probe_request",
                    "fetch_ratio_day", "reconcile_one", "_run_fix_data_body",
                    "_fixed_fill", "_fixed_reconcile", "_fixed_probe",
                    "_internal_port", "_audit_series", "_internal_qualify",
                    "_internal_daily", "_internal_minute", "_pipeline_fill",
                    "_premise_daily", "_premise_qualify", "_premise_request",
                    "_feature_qualify", "_combined_port", "_audit_ticker",
                    "_catchup_port", "_catchup",
                    "_combined_qualify", "_fetch_month_1m", "_fetch_month_1d",
                    "_fixed_audit", "run_raw_group", "run_span_group",
                    "test_end_to_end_store", "test_daily_update_twice",
                    "test_raw_daily_shape", "test_raw_iv_intraday",
                    "test_hvol_daily", "test_iv_rth_only", "test_span_probe",
                    "hammer",
                    "_run_addstock_task"} and child is None:
                raise RequestRefused("shared helper requires an explicit operation child")
            if worker is not None and not isinstance(worker, FetchWorker):
                raise RequestRefused("invalid nightly worker")
            bound = signature.bind(*args, **kwargs)
            # Building a facade may inspect user-defined callback attributes
            # (functools.wraps). Those lookups must not borrow this child either.
            without_authority(guard_callbacks)(bound.arguments)
            token = _WORKER.set(worker)
            cancel_token = _CANCEL.set(bound.arguments.get("cancel"))
            pacer_token = _PACER.set(bound.arguments.get("pacer"))
            governor_token = _GOVERNOR.set(None)
            try:
                return func(*bound.args, **bound.kwargs)
            finally:
                _GOVERNOR.reset(governor_token)
                _PACER.reset(pacer_token)
                _CANCEL.reset(cancel_token)
                _WORKER.reset(token)
    return run


def without_authority(callback):
    """User/UI/provider observers may not borrow the caller's send authority."""
    @wraps(callback)
    def observe(*args, **kwargs):
        variables = (_WORKER, _SYMBOL, _SEND, _METADATA, _GOVERNOR)
        tokens = [(variable, variable.set(None)) for variable in variables]
        try:
            from fetch_operations import observer_scope
            with observer_scope():
                return callback(*args, **kwargs)
        finally:
            for variable, token in reversed(tokens):
                variable.reset(token)
    return observe


@contextmanager
def request_error_observer(callback):
    """Scoped reporting only: installing this hook grants no request rights."""
    token = _REQUEST_ERROR_OBSERVER.set(callback)
    try:
        yield
    finally:
        _REQUEST_ERROR_OBSERVER.reset(token)


def notify_request_error(error):
    callback = _REQUEST_ERROR_OBSERVER.get()
    if callback is None:
        return
    token = _REQUEST_ERROR_OBSERVER.set(None)
    try:
        without_authority(callback)(error)
    except BaseException as observer_error:
        terminal_types = (AuthorityError, LedgerError, RequestCancelled)
        if isinstance(error, terminal_types) or not isinstance(error, Exception):
            raise error
        if isinstance(observer_error, terminal_types) or not isinstance(observer_error, Exception):
            raise
    finally:
        _REQUEST_ERROR_OBSERVER.reset(token)


class _ObserverObject:
    """Event/status facades never lend operation authority to injected code.

    These objects are observers, not trusted fill/audit/reconcile workers.
    Attribute access is isolated too (a property can execute arbitrary code).
    """
    def __init__(self, target):
        self._target = target

    def __call__(self, *args, **kwargs):
        # Some existing cancellation hooks are callables, others are Events.
        # Both interfaces remain observers, including callable descriptors.
        return without_authority(lambda: self._target(*args, **kwargs))()

    def is_set(self):
        def check():
            method = getattr(self._target, "is_set", None)
            return bool(method() if method is not None else self._target())
        return without_authority(check)()

    def __getattr__(self, name):
        value = without_authority(getattr)(self._target, name)
        if not callable(value):
            return value
        observed = without_authority(value)
        if name == "interrupt_event":
            return lambda *args, **kwargs: _observer_object(observed(*args, **kwargs))
        return observed


def _observer_object(value):
    return value if value is None or isinstance(value, _ObserverObject) else _ObserverObject(value)


def observer_object(value):
    """Wrap an injected event/watchdog; this never grants worker rights."""
    return _observer_object(value)


def guard_callbacks(arguments):
    for name in ("progress", "port_status", "on_series", "on_series_start",
                 "on_recover", "port_up", "restart_port", "restart_ports",
                 "adapter_factory", "preflight_factory", "watchdog_factory",
                 "probe_fn", "import_fn"):
        if arguments.get(name) is not None:
            arguments[name] = without_authority(arguments[name])
    for name in ("cancel", "pause", "post_pipeline"):
        if arguments.get(name) is not None:
            arguments[name] = _observer_object(arguments[name])
    if isinstance(arguments.get("kwargs"), dict):
        guard_callbacks(arguments["kwargs"])


def symbol_scope(func):
    signature = inspect.signature(func)
    @wraps(func)
    def run(*args, **kwargs):
        symbol = signature.bind(*args, **kwargs).arguments["ticker"]
        token = _SYMBOL.set(symbol)
        try:
            return func(*args, **kwargs)
        finally:
            _SYMBOL.reset(token)
    return run


def ny_bound(value):
    # Existing planners deliberately use NY-naive wall times. Attach NY here,
    # then use the authority's strict DST/awareness validator at the boundary.
    return aware_ny(value.replace(tzinfo=NY) if value.tzinfo is None else value)


def canonical_token(interval):
    parse_token(interval)
    return interval


def pacer():
    from stock_ibkr import _default_pacer
    return _GOVERNOR.get() or _PACER.get() or _default_pacer()


@dataclass(frozen=True)
class BoundRequest:
    adapter: object
    request: object

    def execute(self, transport, *, acquire_turn, normalizer=None):
        """The live choke cannot substitute a caller's weaker/no-op governor."""
        context = self.request.worker.context
        try:
            governor = self.adapter.request_governor(context)
        except AuthorityError:
            self.request._terminal.set()
            raise
        _GOVERNOR.set(governor)
        def guarded_transport(effective):
            if self.adapter.request_governor(context) is not governor:
                raise RequestRefused("IB account changed while waiting for a turn")
            if search:
                governor.note_search_dispatch()
            return transport(effective)
        envelope = self.request.envelope
        search = envelope.variant == "ibkr-metadata" and envelope.method == "symbol_search"
        metered = (envelope.variant in {"ibkr-bars", "ibkr-bars-unfiltered"}
                   and parse_token(envelope.token)[0].endswith("s"))
        from stock_ibkr import _OrEvent
        cancel = _OrEvent(getattr(acquire_turn, "cancel", None), _CANCEL.get())
        def wait_observer():
            # Discovery/wrapping can invoke properties or callable descriptors,
            # not only the eventual callback. Neither may borrow this worker.
            observer = (getattr(_PACER.get(), "_on_wait", None)
                        or getattr(acquire_turn, "on_wait", None))
            return without_authority(observer) if observer is not None else None
        observed_wait = without_authority(wait_observer)()
        dispatch_wait = 0.0
        def canonical_turn():
            if search:
                return dispatch_wait + governor.wait_search_turn(cancel, on_wait=observed_wait)
            return governor.wait_turn(cancel, metered=metered, on_wait=observed_wait)
        try:
            with (governor.search_dispatch_window(cancel) if search else nullcontext(0.0)) as dispatch_wait:
                return self.request.execute(guarded_transport,
                    acquire_turn=canonical_turn,
                    normalizer=normalizer, governor=governor, cancel=cancel)
        except RequestCancelled as exc:
            from stock_ibkr import Cancelled
            if isinstance(exc, Cancelled):
                raise
            raise Cancelled(str(exc)) from exc


def bind_request(adapter, request):
    return BoundRequest(adapter, request)


def acquire_turn(cancel=None, metered=False):
    from stock_ibkr import _OrEvent
    return pacer().wait_turn(_OrEvent(cancel, _CANCEL.get()), metered=metered)


def unsettled_days(interval, days):
    """Unrequested future suffixes are not evidence of source absence."""
    worker = current_worker()
    if worker is None:
        return set()
    context = worker.context
    horizon = context.horizons[canonical_token(interval)]
    deferred = set()
    for day in days:
        window = context.authority.window(interval, day)
        if window is not None and (horizon is None or window[1] > horizon):
            deferred.add(day)
    return deferred


@contextmanager
def adapter_session(adapter, use_rth):
    # Even ordinary-looking attributes may invoke an injected descriptor.
    previous = without_authority(getattr)(adapter, "use_rth", True)
    without_authority(setattr)(adapter, "use_rth", use_rth)
    terminal = None
    try:
        yield
    except (AuthorityError, LedgerError, RequestCancelled) as exc:
        terminal = exc
        raise
    finally:
        try:
            without_authority(setattr)(adapter, "use_rth", previous)
        except BaseException as cleanup:
            if terminal is not None:
                # Cleanup is still exposed as the cause, but cannot downgrade
                # the operation stop into an ordinary retryable provider error.
                raise terminal from cleanup
            raise


def bar_request(producer, contract, interval, end, duration, *, symbol=None,
                start=None, intended_end=None, variant="ibkr-bars"):
    worker = current_worker()
    if worker is None:
        return None  # Legacy test adapters still work; LiveIB refuses no scope.
    token = canonical_token(interval)
    base, kind, session = parse_token(token)
    raw_end = ny_bound(end)
    first = ny_bound(start) if start is not None else carrier_start(raw_end, duration)
    last = ny_bound(intended_end) if intended_end is not None else raw_end
    if variant not in {"ibkr-bars", "ibkr-bars-unfiltered"}:
        raise RequestRefused("unknown IB bar request variant")
    return worker.request(producer, {
        "variant": variant, "symbol": symbol or _SYMBOL.get() or contract.symbol,
        "con_id": contract.conId, "token": token,
        "what_to_show": KINDS[kind],
        "use_rth": False if variant == "ibkr-bars-unfiltered" else session == "rth",
        "bar_size": BAR_SIZES[base][0], "raw_end": raw_end,
        "duration": duration, "intended_start": first, "intended_end": last})


def head_request(producer, contract, what_to_show, use_rth):
    worker = current_worker()
    if worker is None:
        return None
    return worker.request(producer, {
        "variant": "ibkr-head", "symbol": _SYMBOL.get() or contract.symbol,
        "con_id": contract.conId, "what_to_show": what_to_show, "use_rth": use_rth})


@dataclass(frozen=True)
class TurnOptions:
    callback: object
    cancel: object = None
    on_wait: object = None

    def __call__(self):
        return self.callback()


@dataclass(frozen=True)
class Attempt:
    request: object
    acquire_turn: object


@contextmanager
def send_scope(request, acquire_turn, *, cancel=None, on_wait=None):
    options = TurnOptions(acquire_turn, cancel, on_wait)
    token = _SEND.set(Attempt(request, options) if request is not None else None)
    try:
        yield
    finally:
        _SEND.reset(token)


def take(variant, contract, **fields):
    attempt = _SEND.get()
    _SEND.set(None)  # Consume BEFORE calling any observer or provider callback.
    if attempt is None or attempt.request.envelope.variant != variant:
        raise RequestRefused("no A1 request capability at IB choke point")
    envelope = attempt.request.envelope
    if contract.conId != envelope.con_id:
        raise RequestRefused("contract differs from requested envelope")
    for key, value in fields.items():
        if getattr(envelope, key) != value:
            raise RequestRefused(f"IB {key} differs from requested envelope")
    return attempt


def take_bars(contract, **fields):
    """Consume either exact bar variant at the one guarded IBKR bar choke."""
    attempt = _SEND.get()
    variant = (attempt.request.envelope.variant if attempt is not None
               else "ibkr-bars")
    if variant not in {"ibkr-bars", "ibkr-bars-unfiltered"}:
        variant = "ibkr-bars"  # take() still consumes and refuses the wrong variant.
    if variant == "ibkr-bars-unfiltered":
        # The diagnostic's request carries useRTH=False. LiveIB transports
        # effective.use_rth, so its shared adapter property is not a selector.
        fields.pop("use_rth", None)
    return take(variant, contract, **fields)


@contextmanager
def qualification_scope(method, symbols, acquire_turn, *, cancel=None, on_wait=None):
    worker = current_worker()
    requests = None
    if worker is not None:
        requests = [worker.request(f"ibkr.choke.{method}", {
            "variant": "ibkr-metadata", "symbol": symbol, "con_id": 0,
            "method": "qualification"}) for symbol in symbols]
    token = _METADATA.set((method, tuple(symbols), requests,
                          TurnOptions(acquire_turn, cancel, on_wait)))
    try:
        yield
    finally:
        _METADATA.reset(token)


def take_qualification(method, symbols):
    capability = _METADATA.get()
    _METADATA.set(None)
    if (capability is None or capability[0] != method
            or capability[1] != tuple(symbols) or capability[2] is None):
        raise RequestRefused("no A1 qualification capability at IB choke point")
    return capability[2], capability[3]


def metadata_request(method, symbol, con_id=0):
    worker = current_worker()
    if worker is None:
        raise RequestRefused("metadata requires an explicit operation child")
    return worker.request("ibkr.metadata." + method, {
        "variant": "ibkr-metadata", "symbol": symbol, "con_id": con_id,
        "method": method})


def take_metadata(method, symbol, con_id=0):
    attempt = _SEND.get()
    _SEND.set(None)  # Consume before any attribute lookup on provider objects.
    if attempt is None or attempt.request is None:
        raise RequestRefused("no metadata request capability at IB choke point")
    envelope = attempt.request.envelope
    if (envelope.variant != "ibkr-metadata" or envelope.method != method
            or envelope.symbol != symbol or envelope.con_id != con_id):
        raise RequestRefused("metadata arguments differ from requested envelope")
    return attempt


def canonical_search(text):
    if not isinstance(text, str):
        raise RequestRefused("symbol search text must be a string")
    text = text.strip()[:30].rstrip()
    if not text:
        raise RequestRefused("symbol search text is empty")
    return text


def normalize_company(response, con_id):
    if response is None:
        return {"name": ""}
    if not isinstance(response, (list, tuple)):
        raise RequestRefused("contract details response is not a sequence")
    first_name = ""
    for detail in response:
        pinned = getattr(detail, "contract", None)
        if (pinned is None or type(getattr(pinned, "conId", None)) is not int
                or pinned.conId != con_id or getattr(pinned, "secType", None) != "STK"):
            raise RequestRefused("contract details differ from pinned stock identity")
        name = getattr(detail, "longName", "") or ""
        if not isinstance(name, str):
            raise RequestRefused("contract company name is not text")
        if name.strip() and not first_name:
            first_name = name.strip()
    return {"name": first_name}


def normalize_search(response):
    if response is None:
        response = []
    if not isinstance(response, (list, tuple)):
        raise RequestRefused("symbol search response is not a sequence")
    out = []
    for hit in response:
        contract = getattr(hit, "contract", None)
        if contract is None:
            raise RequestRefused("symbol search result has no contract")
        if getattr(contract, "secType", "") != "STK":
            continue
        con_id = getattr(contract, "conId", None)
        symbol = getattr(contract, "symbol", None)
        if (type(con_id) is not int or con_id <= 0 or not isinstance(symbol, str)
                or not symbol.strip() or symbol != symbol.strip()):
            raise RequestRefused("symbol search returned invalid stock identity")
        row = {"symbol": symbol, "conId": con_id,
            "name": getattr(contract, "description", "") or getattr(hit, "companyName", "") or "",
            "exchange": getattr(contract, "primaryExchange", "") or getattr(contract, "exchange", "") or "",
            "currency": getattr(contract, "currency", "") or ""}
        if any(not isinstance(row[key], str) for key in ("name", "exchange", "currency")):
            raise RequestRefused("symbol search returned malformed metadata")
        out.append(row)
    out.sort(key=lambda row: (row["currency"] != "USD", row["symbol"]))
    return out


def normalize_bars(response, token):
    base, _, _ = parse_token(token)
    rows = []
    for bar in response:
        stamp = bar.date
        if base == "1d":
            if type(stamp) is not date:
                raise RequestRefused("daily IB timestamp must be a calendar date")
            stamp = datetime.combine(stamp, time.min, NY)
        else:
            if not isinstance(stamp, datetime):
                raise RequestRefused("intraday IB timestamp must be aware")
            stamp = aware_ny(stamp)
        row = {"timestamp": stamp.isoformat()}
        for key in ("open", "high", "low", "close", "volume"):
            value = float(getattr(bar, key))
            if not math.isfinite(value):
                raise RequestRefused(f"non-finite IB {key}")
            row[key] = value
        rows.append(row)
    return rows


def publish_bars(rows, token):
    base, _, _ = parse_token(token)
    output = []
    for row in rows:
        stamp = datetime.fromisoformat(row["timestamp"]).astimezone(NY)
        values = {key: row[key] for key in ("open", "high", "low", "close", "volume")}
        output.append(SimpleNamespace(date=stamp.date() if base == "1d" else stamp, **values))
    return output


def normalize_head(response):
    if not isinstance(response, datetime):
        from stock_ibkr import SeriesHalt
        raise SeriesHalt("IBKR returned no usable head timestamp")
    return {"timestamp": aware_ny(response).isoformat()}


def normalize_qualification(response):
    if not isinstance(response, (list, tuple)) or len(response) > 1:
        raise RequestRefused("qualification response is not one contract result")
    out = []
    for contract in response:
        if contract is None:
            continue  # Current ib_async retains an explicit None for unknown inputs.
        conid = contract.conId
        if type(conid) is not int or conid <= 0:
            raise RequestRefused("qualification returned invalid contract identity")
        out.append({"con_id": conid, "symbol": str(contract.symbol),
                    "sec_type": str(contract.secType), "currency": str(contract.currency),
                    "exchange": str(contract.exchange),
                    "primary_exchange": str(getattr(contract, "primaryExchange", "")),
                    "local_symbol": str(getattr(contract, "localSymbol", "")),
                    "trading_class": str(getattr(contract, "tradingClass", ""))})
    return out or None


def refuse_a2():
    raise RequestRefused("producer remains disabled until reviewed Milestone A2")
