"""Process-lifetime account/provider governors and reviewed policy floors.

No adapter, run context, credential material or transport is retained here.
IB account membership must be discovered from the connected adapter by the caller.
"""

from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import hashlib
import math
import re
import threading
import time
from types import MappingProxyType

from fetch_authority import AuthorityError, digest_value


@dataclass(frozen=True)
class Policy:
    min_gap_s: float
    max_requests: int
    window_s: float
    burst_max: int
    burst_window_s: float
    force_metered: bool = False
    backoff_base_s: float = 1.0
    backoff_factor: float = 2.0
    backoff_cap_s: float = 60.0

    def validate(self):
        for name in ("min_gap_s", "window_s", "burst_window_s", "backoff_base_s",
                     "backoff_factor", "backoff_cap_s"):
            value = getattr(self, name)
            if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
                raise AuthorityError("invalid governor policy number")
        if self.backoff_factor < 1 or self.backoff_cap_s < self.backoff_base_s:
            raise AuthorityError("invalid governor backoff")
        if any(type(v) is not int or v < 1 for v in (self.max_requests, self.burst_max)):
            raise AuthorityError("invalid governor request limit")
        if type(self.force_metered) is not bool:
            raise AuthorityError("invalid governor metering policy")
        return self

    @property
    def fingerprint(self):
        return digest_value(asdict(self))


IBKR_POLICY = Policy(0.15, 58, 600.0, 6, 2.0)
HTTP_POLICIES = MappingProxyType({domain: Policy(gap, 60, 60.0, 1, gap, True)
    for domain, gap in (("stockanalysis.com", 1.2), ("yahoo.com", 1.0),
        ("sec.gov", 1.0), ("fortinet.com", 1.0),
        ("githubusercontent.com", 1.0), ("wikipedia.org", 1.0))})


@dataclass(frozen=True)
class Endpoint:
    domain: str
    hosts: tuple[str, ...]
    variant: str
    ranged: bool = False


ENDPOINTS = MappingProxyType({
    "stockanalysis.history": Endpoint("stockanalysis.com", ("stockanalysis.com",), "http-series"),
    "yahoo.splits": Endpoint("yahoo.com", ("query1.finance.yahoo.com", "query2.finance.yahoo.com"), "http-series", True),
    "sec.tickers": Endpoint("sec.gov", ("www.sec.gov",), "http-metadata"),
    "sec.companyfacts": Endpoint("sec.gov", ("data.sec.gov",), "http-metadata"),
    "fortinet.history": Endpoint("fortinet.com", ("www.fortinet.com",), "http-metadata"),
    "sp500.github": Endpoint("githubusercontent.com", ("raw.githubusercontent.com",), "http-metadata"),
    "sp500.wikipedia": Endpoint("wikipedia.org", ("en.wikipedia.org",), "http-metadata"),
})


def endpoint_policy(endpoint_id):
    try:
        endpoint = ENDPOINTS[endpoint_id]
        policy = HTTP_POLICIES[endpoint.domain].validate()
    except (KeyError, TypeError) as exc:
        raise AuthorityError("unknown HTTP endpoint/policy") from exc
    floor = 1.2 if endpoint.domain == "stockanalysis.com" else 1.0
    if (policy.min_gap_s < floor or policy.max_requests > 60 or policy.window_s < 60
            or policy.burst_max != 1 or policy.burst_window_s < floor
            or not policy.force_metered or policy.backoff_base_s < 1
            or policy.backoff_factor < 2 or policy.backoff_cap_s < 60):
        raise AuthorityError("HTTP policy weaker than reviewed floor")
    return endpoint, policy


def verified_account(managed_accounts, selected=None):
    if not isinstance(managed_accounts, (list, tuple)) or not managed_accounts:
        raise AuthorityError("managed account evidence is missing")
    accounts = tuple(managed_accounts)
    for account in accounts:
        if (not isinstance(account, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{1,99}", account)
                or account.casefold() in {"unknown", "none", "null", "public", "placeholder", "test"}):
            raise AuthorityError("managed account evidence is invalid")
    if len(set(accounts)) != len(accounts):
        raise AuthorityError("duplicate managed account evidence")
    if selected is None:
        if len(accounts) != 1:
            raise AuthorityError("explicit account selection required")
        return accounts[0]
    if not isinstance(selected, str) or selected not in accounts:
        raise AuthorityError("selected account is not managed by this connection")
    return selected


def credential_identity(credential=None):
    if credential is None:
        return "public"
    if isinstance(credential, str):
        credential = credential.encode("utf-8")
    if not isinstance(credential, bytes) or not credential:
        raise AuthorityError("invalid credential identity material")
    return "sha256-" + hashlib.sha256(credential).hexdigest()[:32]


def retry_after_seconds(value, now):
    if not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() is None:
        raise AuthorityError("Retry-After clock must be aware")
    if value is None:
        return 0.0
    if not isinstance(value, str) or not value.strip():
        raise AuthorityError("invalid Retry-After")
    value = value.strip()
    try:
        if re.fullmatch(r"[0-9]+", value):
            delay = float(value)
        else:
            deadline = parsedate_to_datetime(value)
            if deadline.tzinfo is None:
                raise ValueError("naive date")
            delay = max(0.0, (deadline - now).total_seconds())
        if not math.isfinite(delay):
            raise ValueError("nonfinite delay")
        return delay
    except (ValueError, TypeError, OverflowError) as exc:
        raise AuthorityError("invalid Retry-After") from exc


def http_saturation(domain, status, retry_after=None):
    return (status == 429 or (status == 503 and retry_after is not None)
            or (domain == "sec.gov" and status == 403))


def http_error_evidence(error):
    """Detach bounded failure metadata; caller runs this as an observer.

    No error-body reads, URL following, stringification or deadline parsing.
    Policy errors are applied only after the original HTTP result is durable.
    """
    status = error.code
    result = {"status": status if type(status) is int and 300 <= status <= 599 else None,
              "retry_after": None}
    if type(status) is not int or not 300 <= status <= 599:
        return {**result, "metadata_error": "invalid HTTP error status"}
    headers = error.headers
    if headers is None:
        return result
    get_all = getattr(headers, "get_all", None)
    if callable(get_all):
        values = get_all("Retry-After", [])
    elif type(headers) is dict and len(headers) <= 128 and all(type(key) is str for key in headers):
        values = [value for key, value in headers.items() if key.lower() == "retry-after"]
    else:
        return {**result, "metadata_error": "unsupported HTTP error headers"}
    if type(values) not in (list, tuple) or len(values) > 1:
        return {**result, "metadata_error": "ambiguous Retry-After headers"}
    value = values[0] if values else None
    if values and (type(value) is not str or len(value) > 256):
        return {**result, "metadata_error": "invalid bounded Retry-After header"}
    result["retry_after"] = value
    return result


class Governor:
    def __init__(self, identity, policy, *, time_fn=time.monotonic, sleep_fn=None):
        from stock_ibkr import Pacer
        self.identity = identity
        self.policy = policy.validate()
        self._search_dispatch_lock = threading.Lock()
        self._pacer = Pacer(max_requests=policy.max_requests, window_s=policy.window_s,
            min_gap_s=policy.min_gap_s, burst_max=policy.burst_max,
            burst_window_s=policy.burst_window_s, force_metered=policy.force_metered,
            time_fn=time_fn, sleep_fn=sleep_fn)

    def wait_turn(self, cancel=None, metered=True, on_wait=None):
        return self._pacer.wait_turn(cancel, metered=metered, on_wait=on_wait)

    def wait_search_turn(self, cancel=None, on_wait=None):
        if self.identity[0] != "ibkr":
            raise AuthorityError("symbol search requires an IBKR account governor")
        # One atomic reservation checks BOTH budgets. No auxiliary reservation,
        # second decision, or per-context throttle can split the account budget.
        return self._pacer.wait_turn(cancel, metered=False, on_wait=on_wait,
                                    symbol_search=True)

    @contextmanager
    def search_dispatch_window(self, cancel=None):
        """Serialize search reservation, decision durability and physical send.

        A reservation alone is not a send timestamp: concurrent decision fsyncs
        can otherwise release two searches together. No second turn is minted.
        """
        from fetch_run_context import RequestCancelled
        if self.identity[0] != "ibkr":
            raise AuthorityError("symbol search requires an IBKR account governor")
        started = self._pacer._time()
        while True:
            if cancel is not None and cancel.is_set():
                raise RequestCancelled("search cancelled before dispatch ownership")
            if self._search_dispatch_lock.acquire(timeout=0.05):
                break
        try:
            if cancel is not None and cancel.is_set():
                raise RequestCancelled("search cancelled before dispatch ownership")
            yield max(0.0, self._pacer._time() - started)
        finally:
            self._search_dispatch_lock.release()

    def note_search_dispatch(self):
        """Move the existing search anchor to send time; do not reserve a turn."""
        with self._pacer._lock:
            self._pacer._last_search = self._pacer._time()

    def saturate(self):
        return self._pacer.saturate()

    def http_backoff(self, status, *, retry_after=None, attempt=1, now=None,
                     max_wait_s=60.0):
        if (type(attempt) is not int or not 1 <= attempt <= 100
                or type(max_wait_s) not in (int, float)
                or not math.isfinite(max_wait_s) or max_wait_s < 0):
            raise AuthorityError("invalid retry budget")
        if self.identity[0] != "http":
            raise AuthorityError("HTTP backoff requires a provider governor")
        policy = self.policy
        local = policy.backoff_base_s
        for _ in range(attempt - 1):
            local = min(policy.backoff_cap_s, local * policy.backoff_factor)
        try:
            server = retry_after_seconds(retry_after, now or datetime.now(timezone.utc))
        except AuthorityError:
            # Unusable metadata cannot erase either saturation or a retryable
            # status's local floor. SEC's separate 403 saturation is preserved.
            saturated = http_saturation(self.identity[1], status, retry_after)
            if saturated:
                self.saturate()
            if saturated or (type(status) is int and (status in (408, 429) or 500 <= status <= 599)):
                self._pacer.defer(local)
            raise
        delay = max(local, server)
        if http_saturation(self.identity[1], status, retry_after):
            self.saturate()
        # Publish the full server deadline even when this operation gives up.
        # Other runs sharing the governor must not retry before that deadline.
        self._pacer.defer(delay)
        if delay > max_wait_s:
            raise AuthorityError("provider retry deadline exceeds operation wait budget")
        return delay

    def evidence(self):
        return {"governor_identity": list(self.identity),
                "governor_policy": asdict(self.policy),
                "governor_policy_fingerprint": self.policy.fingerprint}


class GovernorRegistry:
    def __init__(self, *, time_fn=time.monotonic, sleep_fn=None):
        self._lock = threading.Lock()
        self._entries = {}
        self._time = time_fn
        self._sleep = sleep_fn

    def _get(self, identity, policy):
        with self._lock:
            governor = self._entries.get(identity)
            if governor is None:
                governor = Governor(identity, policy, time_fn=self._time, sleep_fn=self._sleep)
                self._entries[identity] = governor
            elif governor.policy != policy:
                raise AuthorityError("governor policy changed inside process lifetime")
            return governor

    def ibkr(self, managed_accounts, selected=None):
        account = verified_account(managed_accounts, selected)
        policy = IBKR_POLICY.validate()
        if (policy.min_gap_s < 0.15 or policy.max_requests > 58 or policy.window_s < 600
                or policy.burst_max > 6 or policy.burst_window_s < 2):
            raise AuthorityError("IBKR policy weaker than reviewed floor")
        return self._get(("ibkr", account), policy)

    def provider(self, endpoint_id, credential=None):
        endpoint, policy = endpoint_policy(endpoint_id)
        return self._get(("http", endpoint.domain, credential_identity(credential)), policy)


_REGISTRY = GovernorRegistry()


def default_registry():
    return _REGISTRY
