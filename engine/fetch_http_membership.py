"""Finite S&P membership attempts. Helpers own no root or production right."""

from dataclasses import dataclass
import hashlib
import http.client
import math
import time
from types import MappingProxyType
import urllib.request
from urllib.error import HTTPError

from fetch_authority import AuthorityError, ResponseQuarantined, digest_value
from fetch_envelopes import parse_envelope
from fetch_ledger import HTTP_BODY_CAPS, LedgerError
from fetch_run_context import FetchWorker, RequestCancelled, RequestRefused


_BINDINGS = MappingProxyType({
    'http.sp500.github': 'sp500.github',
    'http.sp500.wikipedia': 'sp500.wikipedia',
})
_PATHS = MappingProxyType({
    'main': 'https://raw.githubusercontent.com/datasets/s-and-p-500-companies/main/data/constituents.csv',
    'master': 'https://raw.githubusercontent.com/datasets/s-and-p-500-companies/master/data/constituents.csv',
    'wikipedia': 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies',
})
MEMBERSHIP_EXEMPTION = 'index-membership constituents; no dated series'
_AGENT = 'Mozilla/5.0 (data-bank-tool; sp500 constituents)'
_CAPS = MappingProxyType({'sp500.github': 1024 * 1024,
                         'sp500.wikipedia': 4 * 1024 * 1024})


@dataclass(frozen=True)
class _MembershipEvidence:
    producer: str
    branch: str
    url: str
    headers: tuple


def _spec(producer, branch):
    if type(producer) is not str or producer not in _BINDINGS:
        raise RequestRefused('unknown membership producer')
    choices = ('main', 'master') if producer == 'http.sp500.github' else ('wikipedia',)
    if type(branch) is not str or branch not in choices:
        raise RequestRefused('membership branch is not in the finite catalog')
    headers = (('User-Agent', _AGENT),
               ('Accept', 'text/csv' if producer == 'http.sp500.github' else 'text/html'),
               ('Accept-Encoding', 'identity'))
    envelope = parse_envelope(dict(variant='http-metadata', endpoint=_BINDINGS[producer], subject='SP500'))
    return envelope, _MembershipEvidence(producer, branch, _PATHS[branch], headers)


def verified_membership_attempt_evidence(context, producer, envelope, evidence):
    """Exact typed binding, re-derived before pacing and immediately at the wire."""
    if type(evidence) is not _MembershipEvidence or type(producer) is not str:
        raise RequestRefused('membership attempt provenance is missing')
    if (type(evidence.producer) is not str or type(evidence.branch) is not str
            or type(evidence.url) is not str or type(evidence.headers) is not tuple
            or len(evidence.headers) != 3
            or any(type(pair) is not tuple or len(pair) != 2
                   or any(type(value) is not str for value in pair)
                   for pair in evidence.headers)):
        raise RequestRefused('membership attempt provenance changed: fields are not exact')
    if evidence.producer != producer:
        raise RequestRefused('membership attempt provenance is missing')
    expected_envelope, expected = _spec(producer, evidence.branch)
    if envelope != expected_envelope or evidence != expected:
        raise RequestRefused('membership attempt provenance changed')
    if HTTP_BODY_CAPS.get(envelope.endpoint) != _CAPS[envelope.endpoint]:
        raise RequestRefused('membership reviewed body cap changed')
    return dict(producer=producer, endpoint=envelope.endpoint, subject='SP500',
        branch=evidence.branch, method='GET', url=evidence.url,
        header_digests={key: hashlib.sha256(value.encode('ascii')).hexdigest()
                        for key, value in evidence.headers},
        body_cap_bytes=HTTP_BODY_CAPS[envelope.endpoint],
        metadata_exemption=MEMBERSHIP_EXEMPTION)


def normalize_membership(raw, envelope):
    """Parsing belongs inside the executor, before its durable result release."""
    from sp500 import parse_datahub, parse_wikipedia
    if (envelope.variant != 'http-metadata' or envelope.subject != 'SP500'
            or envelope.endpoint not in _BINDINGS.values()
            or type(raw) is not bytes or not raw
            or len(raw) > HTTP_BODY_CAPS[envelope.endpoint]):
        raise ResponseQuarantined('membership response is not bounded metadata')
    try:
        text = raw.decode('utf-8')
    except UnicodeDecodeError as error:
        raise ResponseQuarantined('membership response is not UTF-8') from error
    return parse_datahub(text) if envelope.endpoint == 'sp500.github' else parse_wikipedia(text)


def guarded_membership_attempt(worker, producer, branch, send=None, *, timeout=15,
                               cancel=None, _provenance=None, _root_deadline=None):
    """Exactly one physical attempt, one fresh logical ID, no hidden retry."""
    from fetch_test_policy import is_live_capability
    if type(worker) is not FetchWorker:
        raise RequestRefused('membership requires an explicit operation worker')
    context = worker.context
    if ((send is not None or context.test_capability is not None)
            and not is_live_capability(context.test_capability)):
        raise RequestRefused('membership injected transport requires offline policy')
    if send is not None and not callable(send):
        raise RequestRefused('membership sender is missing')
    if _provenance is not None and type(_provenance) is not dict:
        raise RequestRefused('membership capture requires an owned dictionary')
    if type(timeout) not in (int, float) or not math.isfinite(timeout) or not 0 < timeout <= 120:
        raise RequestRefused('membership timeout is invalid')
    if (_root_deadline is not None and (type(_root_deadline) is not float
            or not math.isfinite(_root_deadline))):
        raise RequestRefused('membership root deadline is not detached')
    envelope, evidence = _spec(producer, branch)
    request = worker.request(producer, envelope.wire(), _http_binding=evidence)
    cap = HTTP_BODY_CAPS[envelope.endpoint]
    capture = {}

    def one_send(effective):
        if effective != envelope:
            raise RequestRefused('membership effective request changed before transport')
        verified_membership_attempt_evidence(context, producer, effective, evidence)
        wire_timeout = timeout
        if _root_deadline is not None:
            # Governor waiting consumes the root budget too. Do not restart an
            # already-consumed timeout when the physical send finally gets its turn.
            remaining_root = _root_deadline - time.monotonic()
            if remaining_root <= 0:
                raise RequestRefused('membership root deadline expired before wire')
            wire_timeout = min(timeout, remaining_root)
        if send is not None:
            from fetch_ibkr_bridge import without_authority
            raw = without_authority(send)(evidence.url)
            if type(raw) is bytes:
                raw = raw[:cap + 1]
        else:
            from fetch_http_transport import Deadline, build_opener
            response, primary = None, None
            with Deadline(wire_timeout, endpoint=envelope.endpoint) as deadline:
                try:
                    opener = build_opener(deadline)
                    wire = urllib.request.Request(evidence.url, headers=dict(evidence.headers), method='GET')
                    response = urllib.request.OpenerDirector.open(opener, wire, timeout=deadline.remaining())
                    deadline.remaining()
                    body = bytearray()
                    while len(body) <= cap:
                        if cancel is not None and cancel.is_set():
                            raise RequestCancelled('membership body read cancelled')
                        deadline.remaining()
                        remaining = min(65536, cap + 1 - len(body))
                        part = response.read1(remaining)
                        deadline.remaining()
                        if type(part) is not bytes:
                            raise RequestRefused('membership response chunk is not bytes')
                        body.extend(part[:remaining])
                        if not part:
                            break
                    raw = bytes(body)
                except HTTPError as error:
                    primary, response = error, error
                    deadline.remaining()
                    raise
                except OSError as error:
                    primary = error
                    try:
                        deadline.remaining()
                    except TimeoutError as expired:
                        raise expired from error
                    # HTTPResponse.read1 can close its owned file when the
                    # declared body ends. A close failure then surfaces from
                    # read1 with fp already cleared, not from our finally.
                    # It is cleanup debt, never a recoverable provider error.
                    if (not isinstance(error, TimeoutError)
                            and isinstance(response, http.client.HTTPResponse) and response.isclosed()):
                        primary = LedgerError('membership response cleanup failed during body read')
                        raise primary from error
                    raise
                except BaseException as error:
                    primary = error
                    raise
                finally:
                    if response is not None:
                        try:
                            response.close()
                        except BaseException as close_error:
                            if not (isinstance(primary, (AuthorityError, LedgerError,
                                        RequestCancelled, ResponseQuarantined)) or
                                    (primary is not None and not isinstance(primary, Exception))):
                                raise LedgerError('membership response cleanup failed') from close_error
        if type(raw) is bytes:
            capture.update(raw_digest=hashlib.sha256(raw).hexdigest(), observed_bytes=len(raw))
        return raw

    accepted = request.execute(one_send, acquire_turn=lambda: 0,
        governor=context.governors.provider(envelope.endpoint),
        normalizer=lambda raw: normalize_membership(raw, envelope),
        attempt_evidence=evidence, cancel=cancel)
    if _provenance is not None:
        _provenance.update(capture, operation_id=context.operation_id,
            worker_id=worker.worker_id, logical_id=request.logical_id,
            accepted_digest=digest_value(accepted),
            request_provenance=verified_membership_attempt_evidence(
                context, producer, envelope, evidence))
    return accepted
