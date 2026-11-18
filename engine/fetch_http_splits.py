"""Finite split endpoint bridge. No operation root or activation API lives here.

Each default/injected attempt crosses the shipped executor and canonical
governor. Only detached accepted evidence leaves it after result durability.
"""

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from itertools import count
import hashlib
import math
import re
from types import MappingProxyType
import urllib.request
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode

from fetch_authority import (AuthorityError, CalendarUnsupported,
    ResponseQuarantined, canonical_bytes, strict_json)
from fetch_envelopes import parse_envelope
from fetch_ledger import HTTP_BODY_CAPS, LedgerError
from fetch_run_context import FetchWorker, LogicalRequest, RequestCancelled, RequestRefused


_BINDINGS = MappingProxyType({
    "http.split_provider.yahoo": ("yahoo.splits", "http-series"),
    "http.split_provider.sec_tickers": ("sec.tickers", "http-metadata"),
    "http.split_provider.sec_facts": ("sec.companyfacts", "http-metadata"),
    "http.split_provider.issuer": ("fortinet.history", "http-metadata"),
})
_EXEMPTIONS = MappingProxyType({
    "sec.tickers": "symbol-to-CIK mapping; no dated series",
    "sec.companyfacts": "past-filing corporate facts; never bars",
    "fortinet.history": "issuer boolean history evidence; no dated series",
})
_SYMBOL = re.compile(r"[A-Z0-9][A-Z0-9.-]{0,29}\Z")
_CONTACT = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,63}")
_RECORD = re.compile(r"[A-Za-z0-9_.:-]{1,64}\Z")
_AGENT = "EMA-DataBank/1.0"
_MAX_EVENTS = 20000


def validate_sec_user_agent(value):
    """Validate explicit engine configuration, with no value in diagnostics."""
    if (type(value) is not str or not 1 <= len(value) <= 256
            or any(not 32 <= ord(char) <= 126 for char in value)
            or _CONTACT.search(value) is None):
        raise RequestRefused("SEC_USER_AGENT is missing or invalid")
    return value


def _symbol(value):
    if type(value) is not str or not _SYMBOL.fullmatch(value):
        raise RequestRefused("split provider symbol is not canonical")
    return value


def _cik(value):
    if type(value) is not int or not 0 < value <= 9999999999:
        raise RequestRefused("split CIK is not a positive ten-digit identity")
    return value


def yahoo_bounds(context):
    day = max(date(1980, 1, 1), context.authority.first_date)
    while day <= context.authority.valid_through:
        window = context.authority.window("1d", day)
        if window is not None:
            start = window[0]
            break
        day += timedelta(days=1)
    else:
        raise RequestRefused("Yahoo has no supported start session")
    end = context.horizons["1d"]
    if end is None or start >= end:
        raise RequestRefused("Yahoo has no settled range")
    return start, end


@dataclass(frozen=True)
class _SplitEvidence:
    producer: str
    symbol: str
    cik: object
    sec_agent: object = field(repr=False)
    url: str
    headers: tuple = field(repr=False)


@dataclass(frozen=True)
class _SplitRequest:
    request: object
    evidence: _SplitEvidence
    _attempts: object = field(default_factory=lambda: count(1), repr=False, compare=False)


def _spec(context, producer, symbol, cik, sec_agent):
    from split_provider import SEC_TICKERS_URL, SEC_FACTS_URL, FTNT_ISSUER_URL
    symbol = _symbol(symbol)
    if type(producer) is not str or producer not in _BINDINGS:
        raise RequestRefused("unknown split physical producer")
    endpoint, variant = _BINDINGS[producer]
    if endpoint != "sec.companyfacts" and cik is not None:
        raise RequestRefused("CIK belongs only to the CompanyFacts request")
    if endpoint.startswith("sec."):
        agent = validate_sec_user_agent(sec_agent)
    else:
        if sec_agent is not None:
            raise RequestRefused("SEC header belongs only to SEC requests")
        agent = _AGENT
    headers = (("User-Agent", agent),
               ("Accept", "text/html" if endpoint == "fortinet.history" else "application/json"),
               ("Accept-Encoding", "identity"))
    envelope = {"variant": variant, "endpoint": endpoint}
    if endpoint == "yahoo.splits":
        start, end = yahoo_bounds(context)
        envelope.update(subject=symbol, token="1d", intended_start=start, intended_end=end)
        query = urlencode((('period1', int(start.timestamp())), ('period2', int(end.timestamp())),
                           ('interval', '1d'), ('events', 'splits'), ('includeAdjustedClose', 'true')))
        url = "https://query1.finance.yahoo.com/v8/finance/chart/" + quote(symbol, safe=".-") + "?" + query
    elif endpoint == "sec.tickers":
        envelope["subject"] = "SEC:TICKERS"
        url = SEC_TICKERS_URL
    elif endpoint == "sec.companyfacts":
        cik = _cik(cik)
        envelope["subject"] = f"CIK:{cik:010d}"
        url = SEC_FACTS_URL.format(cik=cik)
    else:
        if symbol != "FTNT":
            raise RequestRefused("issuer evidence is FTNT-only")
        envelope["subject"] = "FTNT"
        url = FTNT_ISSUER_URL
    return parse_envelope(envelope), _SplitEvidence(producer, symbol, cik, sec_agent, url, headers)


def verified_split_attempt_evidence(context, producer, envelope, evidence):
    """Re-derive wire provenance BEFORE the governor and durable decision."""
    if type(evidence) is not _SplitEvidence or type(producer) is not str:
        raise RequestRefused("split attempt provenance is missing")
    if (type(evidence.producer) is not str or type(evidence.symbol) is not str
            or type(evidence.url) is not str or type(evidence.headers) is not tuple
            or len(evidence.headers) != 3
            or any(type(pair) is not tuple or len(pair) != 2
                   or any(type(value) is not str for value in pair)
                   for pair in evidence.headers)
            or (evidence.cik is not None and type(evidence.cik) is not int)
            or (evidence.sec_agent is not None and type(evidence.sec_agent) is not str)):
        raise RequestRefused("split attempt provenance changed: fields are not exact")
    if evidence.producer != producer:
        raise RequestRefused("split attempt provenance is missing")
    expected_envelope, expected = _spec(context, producer, evidence.symbol, evidence.cik, evidence.sec_agent)
    if envelope != expected_envelope or evidence != expected:
        raise RequestRefused("split attempt provenance changed")
    result = {"producer": producer, "endpoint": envelope.endpoint,
              "subject": envelope.subject, "requested_symbol": evidence.symbol,
              "method": "GET", "url": evidence.url,
              "header_digests": {key: hashlib.sha256(value.encode('ascii')).hexdigest()
                                 for key, value in evidence.headers},
              "body_cap_bytes": HTTP_BODY_CAPS[envelope.endpoint]}
    if envelope.endpoint == "yahoo.splits":
        result.update(requested_start_date="1980-01-01",
            truncated_coverage=context.authority.first_date > date(1980, 1, 1),
            period1=int(envelope.intended_start.timestamp()),
            period2=int(envelope.intended_end.timestamp()), period2_exclusive=True)
    else:
        result['metadata_exemption'] = _EXEMPTIONS[envelope.endpoint]
    return result


def split_request(worker, producer, symbol, *, cik=None, sec_user_agent=None):
    if type(worker) is not FetchWorker:
        raise RequestRefused("split request requires an operation worker")
    envelope, evidence = _spec(worker.context, producer, symbol, cik, sec_user_agent)
    request = worker.request(producer, envelope.wire(), _http_binding=evidence)
    return _SplitRequest(request, evidence)


def guarded_split_attempt(worker, producer, symbol, send=None, *, cik=None,
                          sec_user_agent=None, cancel=None, timeout=20.0,
                          _logical_request=None, _capture=None, _provenance=None):
    """ONE physical attempt; an injected sender is a confined raw-byte call."""
    from fetch_test_policy import is_live_capability
    if type(worker) is not FetchWorker:
        raise RequestRefused("split attempt requires an operation worker")
    context = worker.context
    if ((send is not None or context.test_capability is not None)
            and not is_live_capability(context.test_capability)):
        raise RequestRefused("split injected transport requires offline policy")
    if send is not None and not callable(send):
        raise RequestRefused("split sender is missing")
    if _capture is not None and type(_capture) is not dict:
        raise RequestRefused("split capture must be an owned dictionary")
    if _provenance is not None and type(_provenance) is not dict:
        raise RequestRefused("split provenance must be an owned dictionary")
    if type(timeout) not in (int, float) or not math.isfinite(timeout) or not 0 < timeout <= 120:
        raise RequestRefused("split timeout is invalid")
    envelope, evidence = _spec(context, producer, symbol, cik, sec_user_agent)
    retained = (split_request(worker, producer, symbol, cik=cik, sec_user_agent=sec_user_agent)
                if _logical_request is None else _logical_request)
    if (type(retained) is not _SplitRequest or retained.evidence != evidence
            or type(retained.request) is not LogicalRequest
            or retained.request.worker is not worker or retained.request.producer_id != producer
            or retained.request.envelope != envelope):
        raise RequestRefused("split retry request binding differs")
    if not context.permits(producer, envelope):
        raise RequestRefused("split request admission refused")
    if next(retained._attempts) > 3:
        raise RequestRefused("split logical request exceeds three attempts")
    cap = HTTP_BODY_CAPS[envelope.endpoint]
    governor = context.governors.provider(envelope.endpoint)

    def one_send(effective):
        if effective != envelope:
            raise RequestRefused("split effective request changed before transport")
        verified_split_attempt_evidence(context, producer, effective, evidence)
        if send is not None:
            raw = send(evidence.url)
            if isinstance(raw, bytes):
                raw = raw[:cap + 1]
        else:
            from fetch_http_transport import Deadline, build_opener
            response = None
            with Deadline(timeout, endpoint=envelope.endpoint) as deadline:
                primary_error = None
                try:
                    opener = build_opener(deadline)
                    wire = urllib.request.Request(evidence.url, headers=dict(evidence.headers), method="GET")
                    response = urllib.request.OpenerDirector.open(opener, wire, timeout=deadline.remaining())
                    deadline.remaining()
                    body = bytearray()
                    while len(body) <= cap:
                        if cancel is not None and cancel.is_set():
                            raise RequestCancelled("split body read cancelled")
                        deadline.remaining()
                        remaining = min(65536, cap + 1 - len(body))
                        part = response.read1(remaining)
                        deadline.remaining()
                        if not isinstance(part, bytes):
                            raise RequestRefused("split response chunk is not bytes")
                        body.extend(part[:remaining])
                        if not part:
                            break
                    raw = bytes(body)
                except HTTPError as error:
                    primary_error = error
                    response = error
                    deadline.remaining()
                    raise
                except OSError as error:
                    primary_error = error
                    try:
                        deadline.remaining()
                    except TimeoutError as expired:
                        raise expired from error
                    raise
                except BaseException as error:
                    primary_error = error
                    raise
                finally:
                    if response is not None:
                        try:
                            response.close()
                        except BaseException as close_error:
                            if not (isinstance(primary_error, (AuthorityError, LedgerError,
                                        RequestCancelled, ResponseQuarantined)) or (
                                        primary_error is not None and not isinstance(primary_error, Exception))):
                                raise LedgerError("split response cleanup failed") from close_error
        if _capture is not None and isinstance(raw, bytes):
            _capture["raw_digest"] = hashlib.sha256(raw).hexdigest()
            _capture["observed_bytes"] = len(raw)
        return raw

    accepted = retained.request.execute(one_send, acquire_turn=lambda: 0, governor=governor,
        normalizer=decode_split_json, attempt_evidence=evidence, cancel=cancel)
    if _provenance is not None:
        # A successful capture is detached provenance, not authority or a seal.
        # Only populate it after the executor's durable result and final check.
        from fetch_authority import digest_value
        _provenance.update(operation_id=context.operation_id,
                        logical_id=retained.request.logical_id,
                        accepted_digest=digest_value(accepted),
                        request_provenance=verified_split_attempt_evidence(
                            context, producer, envelope, evidence))
    return accepted


def guarded_split_fetch(worker, producer, symbol, send=None, *, attempts=2, **options):
    """A finite governed sequence; no local sleeps, fallback host or hidden retry."""
    if type(attempts) is not int or not 1 <= attempts <= 3:
        raise RequestRefused("split retry cap is invalid")
    retained = split_request(worker, producer, symbol, cik=options.get('cik'),
                             sec_user_agent=options.get('sec_user_agent'))
    for index in range(attempts):
        try:
            return guarded_split_attempt(worker, producer, symbol, send,
                _logical_request=retained, **options)
        except (AuthorityError, LedgerError, RequestCancelled, ResponseQuarantined):
            raise
        except (TimeoutError, OSError) as error:
            if isinstance(error, HTTPError):
                retry = error.code in (408, 429) or 500 <= error.code <= 599 or (
                    retained.request.envelope.endpoint.startswith('sec.') and error.code == 403)
            elif isinstance(error, URLError):
                if isinstance(error.reason, (AuthorityError, LedgerError, RequestCancelled)):
                    raise error.reason from error
                retry = isinstance(error.reason, (TimeoutError, OSError))
            else:
                retry = True
            if not retry or index + 1 == attempts:
                raise


def decode_split_json(raw, *, max_bytes=16 * 1024 * 1024):
    try:
        if not isinstance(raw, bytes) or not raw or len(raw) > max_bytes:
            raise ValueError()
        payload = strict_json(raw)
        canonical_bytes(payload)  # Reject exponent overflow as well as NaN tokens.
        return payload
    except (AuthorityError, ValueError, TypeError, OverflowError, RecursionError) as exc:
        raise ResponseQuarantined("split response is not strict finite JSON") from exc


def _positive(value):
    if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
        raise ResponseQuarantined("split ratio component is not finite and positive")
    return float(value)


def normalize_yahoo_events(payload):
    try:
        chart = payload['chart']
        if chart.get('error') is not None or len(chart['result']) != 1:
            raise ValueError()
        container = chart['result'][0]
        events = container.get('events')
        events = {} if events is None else events
        if type(events) is not dict:
            raise ValueError()
        raw_events = events.get('splits')
        raw_events = {} if raw_events is None else raw_events
        if type(raw_events) is not dict or len(raw_events) > _MAX_EVENTS:
            raise ValueError()
        result, seen = [], set()
        for record, raw in sorted(raw_events.items()):
            if type(record) is not str or not _RECORD.fullmatch(record) or type(raw) is not dict:
                raise ValueError()
            epoch = raw['date']
            if type(epoch) is not int:
                raise ValueError()
            label = datetime.fromtimestamp(epoch, timezone.utc).date().isoformat()
            numerator, denominator = _positive(raw['numerator']), _positive(raw['denominator'])
            ratio = numerator / denominator
            if not math.isfinite(ratio) or ratio <= 0 or label in seen:
                raise ValueError()
            seen.add(label)
            result.append({'record_id': record, 'source_epoch': epoch, 'label': label,
                           'ex_date': label, 'numerator': numerator, 'denominator': denominator,
                           'ratio': ratio, 'ratio_convention': 'new_shares_per_old_share'})
        return result
    except (KeyError, IndexError, AttributeError, TypeError, ValueError, OverflowError, OSError) as exc:
        raise ResponseQuarantined("Yahoo split events are malformed or duplicated") from exc


def filter_yahoo_events(rows, envelope, context):
    accepted = []
    horizon = context.horizons['1d']
    for row in rows:
        day = date.fromisoformat(row['label'])
        try:
            window = context.authority.window('1d', day)
            if window is None:
                continue
            close = window[1]
            if (horizon is None or close > horizon
                    or context.captured_now < context.authority.daily_settled_close(day)
                    or day < envelope.intended_start.date() or day > envelope.intended_end.date()
                    or close > envelope.intended_end):
                continue
        except CalendarUnsupported:
            continue
        accepted.append({**row, 'timestamp': close.astimezone(timezone.utc).isoformat()})
    return accepted


def _metadata_cik(value):
    """Finite JSON identity conversion; never bool, float or arbitrary int()."""
    if type(value) is str and re.fullmatch(r'[0-9]{1,10}', value):
        value = int(value)
    if type(value) is not int or not 0 < value <= 9999999999:
        raise ValueError('invalid metadata CIK')
    return value


def _metadata_object(value):
    if value is None:
        return {}
    if type(value) is not dict:
        raise ValueError('invalid metadata container')
    return value


def _metadata_row_evidence(capture, observed, accepted, reasons, normalized=0, **identity):
    if capture is not None:
        capture.update(observed_count=observed, accepted_count=accepted,
                       dropped_count=observed - accepted,
                       dropped_reasons=dict(sorted(reasons.items())),
                       normalized_count=normalized, **identity)


def _fact_drop_reason(row):
    if type(row) is not dict:
        return 'non_object'
    try:
        _positive(row.get('val'))
    except (ResponseQuarantined, OverflowError):
        return 'invalid_ratio'
    for key in ('end', 'filed'):
        value = row.get(key)
        if key == 'filed' and value is None:
            continue
        try:
            if type(value) is not str or date.fromisoformat(value).isoformat() != value:
                return 'invalid_' + key
        except ValueError:
            return 'invalid_' + key
    for key, limit in (('form', 16), ('accn', 64)):
        value = row.get(key)
        if value is not None and (type(value) is not str or len(value) > limit
                or any(ord(char) < 32 or ord(char) > 126 for char in value)):
            return 'invalid_' + key
    return None


def normalize_split_metadata(raw, envelope, *, _capture=None):
    from split_provider import parse_sec_split_facts, parse_ftnt_issuer_evidence, ProviderError, SEC_SPLIT_TAG
    if _capture is not None and type(_capture) is not dict:
        raise RequestRefused('metadata capture requires an owned dictionary')
    try:
        if not isinstance(raw, bytes) or not raw or len(raw) > HTTP_BODY_CAPS[envelope.endpoint]:
            raise ValueError()
        if envelope.endpoint == 'fortinet.history':
            return parse_ftnt_issuer_evidence(raw)
        payload = decode_split_json(raw)
        if type(payload) is not dict:
            raise ValueError()
        if envelope.endpoint == 'sec.tickers':
            if len(payload) > _MAX_EVENTS:
                raise ValueError()
            mapping, reasons, normalized = {}, {}, 0
            for row in payload.values():
                reason = None
                if type(row) is not dict:
                    reason = 'non_object'
                elif type(row.get('ticker')) is not str or not row['ticker'].isascii():
                    reason = 'invalid_ticker'
                else:
                    try:
                        symbol = _symbol(row['ticker'].strip(' \t\r\n').upper())
                    except RequestRefused:
                        reason = 'invalid_ticker'
                    if reason is None:
                        try:
                            cik = _metadata_cik(row.get('cik_str'))
                        except ValueError:
                            reason = 'invalid_cik'
                if reason is not None:
                    reasons[reason] = reasons.get(reason, 0) + 1
                    continue
                if symbol in mapping:
                    if mapping[symbol] != cik:
                        raise ValueError('conflicting ticker identity')
                    reasons['duplicate_identity'] = reasons.get('duplicate_identity', 0) + 1
                    continue
                mapping[symbol] = cik
                normalized += int(symbol != row['ticker'] or type(row['cik_str']) is str)
            if not mapping:
                raise ValueError()
            _metadata_row_evidence(_capture, len(payload), len(mapping), reasons, normalized)
            return mapping
        expected_cik = int(envelope.subject.removeprefix('CIK:'))
        payload_cik = _metadata_cik(payload.get('cik'))
        if payload_cik != expected_cik:
            raise ValueError()
        facts_container = _metadata_object(payload.get('facts'))
        gaap = _metadata_object(facts_container.get('us-gaap'))
        tag = _metadata_object(gaap.get(SEC_SPLIT_TAG))
        unit_container = _metadata_object(tag.get('units'))
        units = unit_container.get('pure')
        units = [] if units is None else units
        if type(units) is not list or len(units) > _MAX_EVENTS:
            raise ValueError()
        sanitized, reasons, seen = [], {}, set()
        for row in units:
            reason = _fact_drop_reason(row)
            if reason is not None:
                reasons[reason] = reasons.get(reason, 0) + 1
                continue
            key = (_positive(row['val']), row['end'], (row.get('accn') or '').strip())
            if key in seen:
                reasons['duplicate_fact'] = reasons.get('duplicate_fact', 0) + 1
                continue
            seen.add(key)
            sanitized.append({name: row.get(name) for name in ('val', 'end', 'filed', 'form', 'accn')})
        clean_payload = {'facts': {'us-gaap': {SEC_SPLIT_TAG: {'units': {'pure': sanitized}}}}}
        facts, records = parse_sec_split_facts(clean_payload, envelope.subject, hashlib.sha256(raw).hexdigest())
        if len(facts) != len(sanitized):
            raise ValueError('conflicting or lost fact identity')
        # A filing can legitimately describe several splits. Its legacy source
        # ID cannot attest more than one fact: drop the entire colliding group,
        # not clean evidence from other filings, and never retain its last writer.
        source_counts = {}
        for fact in facts:
            source_id = fact['source_id']
            source_counts[source_id] = source_counts.get(source_id, 0) + 1
        colliding = {source_id for source_id, count in source_counts.items() if count > 1}
        if colliding:
            kept = [fact for fact in facts if fact['source_id'] not in colliding]
            reasons['source_identity_collision'] = len(facts) - len(kept)
            facts = kept
            records = {source_id: record for source_id, record in records.items()
                       if source_id not in colliding}
        if len(records) != len(facts) or set(records) != {fact['source_id'] for fact in facts}:
            raise ValueError('conflicting or lost fact identity')
        _metadata_row_evidence(_capture, len(units), len(facts), reasons,
                               payload_identity_normalized=type(payload['cik']) is str,
                               colliding_identity_count=len(colliding))
        return {'cik': expected_cik, 'split_facts': facts, 'records': records}
    except (AuthorityError, ProviderError, KeyError, AttributeError, TypeError, ValueError, OverflowError, RecursionError) as exc:
        raise ResponseQuarantined("split metadata is malformed or identity-unsafe") from exc
