"""Finite daily-label rules for held A2 HTTP series attempts.

The legacy IBKR bar path uses provider timestamps. StockAnalysis supplies a
date label instead: deriving its session close is an authority decision, not a
timestamp invented by the caller or the raw transport.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from dataclasses import dataclass
import json
import hashlib
import math
import re
import urllib.request
from urllib.error import HTTPError
from urllib.parse import quote, urlsplit, parse_qsl

from fetch_authority import (AuthorityError, CalendarUnsupported,
                             ResponseQuarantined, aware_ny, strict_json)


_DATE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}\Z")
_OHLCV = ("o", "h", "l", "c", "v")
_SYMBOL = re.compile(r"[A-Za-z0-9][A-Za-z0-9.-]{0,29}\Z")
_MAX_HISTORY_BYTES = 8 * 1024 * 1024
_MAX_HISTORY_ROWS = 20_000
_MAX_SOURCE_DATE_JSON_BYTES = 64


def stockanalysis_url(provider_symbol: str, rng: str) -> str:
    """One exact host/path/query; a callback cannot supply another endpoint."""
    if not isinstance(provider_symbol, str) or not _SYMBOL.fullmatch(provider_symbol):
        raise AuthorityError("invalid StockAnalysis provider symbol")
    if rng not in {"5Y", "Max"}:
        raise AuthorityError("unsupported StockAnalysis range")
    url = ("https://stockanalysis.com/api/symbol/s/"
           + quote(provider_symbol, safe=".-")
           + "/history?range=" + rng + "&period=Daily")
    parsed = urlsplit(url)
    if (parsed.scheme != "https" or parsed.hostname != "stockanalysis.com"
            or parsed.username is not None or parsed.password is not None
            or parsed.fragment or parsed.path !=
            f"/api/symbol/s/{quote(provider_symbol, safe='.-')}/history"
            or parse_qsl(parsed.query, keep_blank_values=True) !=
            [("range", rng), ("period", "Daily")]):
        raise AuthorityError("StockAnalysis URL is not endpoint-fixed")
    return url


@dataclass(frozen=True)
class _StockAnalysisRequest:
    request: object
    url: str
    range: str


def stockanalysis_request(worker, producer_id, provider_symbol, rng):
    """Create one caller-owned logical request for a bounded retry sequence."""
    from fetch_run_context import FetchWorker, RequestRefused
    from fetch_test_policy import is_live_capability

    if type(worker) is not FetchWorker:
        raise RequestRefused("StockAnalysis request requires an operation worker")
    if worker.context.test_capability is not None and not is_live_capability(worker.context.test_capability):
        raise RequestRefused("StockAnalysis test capability expired")
    if producer_id not in {"http.stockanalysis.validation", "http.stockanalysis.external_sweep"}:
        raise RequestRefused("unknown StockAnalysis physical producer")
    url = stockanalysis_url(provider_symbol, rng)
    bounds = stockanalysis_bounds(worker.context.authority, worker.context.captured_now, rng)
    request = worker.request(producer_id, {
        "variant": "http-series", "endpoint": "stockanalysis.history",
        "subject": provider_symbol, "token": "1d",
        "intended_start": bounds["intended_start"],
        "intended_end": bounds["intended_end"]})
    return _StockAnalysisRequest(request, url, rng)


def guarded_stockanalysis_attempt(worker, producer_id, provider_symbol, rng, send=None,
                                  *, cancel=None, _logical_request=None,
                                  timeout=20.0, _capture=None):
    """One guarded physical attempt; `send(url)` is a one-call transport.

    An injected sender requires the confined offline capability. No sender
    selects the owned default; unchanged operation admission still holds A2.
    """
    from fetch_run_context import FetchWorker, LogicalRequest, RequestRefused
    from fetch_envelopes import parse_envelope
    from fetch_test_policy import is_live_capability

    if type(worker) is not FetchWorker:
        raise RequestRefused("StockAnalysis attempt requires an operation worker")
    context = worker.context
    if ((context.test_capability is not None or send is not None)
            and not is_live_capability(context.test_capability)):
        raise RequestRefused("StockAnalysis injected transport requires offline policy")
    if producer_id not in {"http.stockanalysis.validation",
                           "http.stockanalysis.external_sweep"}:
        raise RequestRefused("unknown StockAnalysis physical producer")
    if send is not None and not callable(send):
        raise RequestRefused("StockAnalysis sender is missing")
    if _capture is not None and type(_capture) is not dict:
        raise RequestRefused("StockAnalysis response capture must be an owned dictionary")
    if type(timeout) not in (int, float) or not math.isfinite(timeout) or not 0 < timeout <= 120:
        raise RequestRefused("StockAnalysis timeout is invalid")
    url = stockanalysis_url(provider_symbol, rng)
    bounds = stockanalysis_bounds(context.authority, context.captured_now, rng)
    evidence = {
        "url": url, "range": rng,
        "requested_start_date": bounds["requested_start_date"],
        "truncated_coverage": bounds["truncated_coverage"],
    }
    envelope = parse_envelope({
        "variant": "http-series", "endpoint": "stockanalysis.history",
        "subject": provider_symbol, "token": "1d",
        "intended_start": bounds["intended_start"],
        "intended_end": bounds["intended_end"]})
    retained = (stockanalysis_request(worker, producer_id, provider_symbol, rng)
                if _logical_request is None else _logical_request)
    if (type(retained) is not _StockAnalysisRequest or retained.url != url
            or retained.range != rng):
        raise RequestRefused("StockAnalysis retry request binding differs")
    request = retained.request
    if (type(request) is not LogicalRequest or request.worker is not worker
            or request.producer_id != producer_id or request.envelope != envelope):
        raise RequestRefused("StockAnalysis retry request binding differs")
    if not context.permits(producer_id, envelope):
        raise RequestRefused("StockAnalysis retry request admission refused")
    governor = context.governors.provider("stockanalysis.history")

    def one_send(effective):
        if (effective.variant != "http-series"
                or effective.endpoint != "stockanalysis.history"
                or effective.subject != provider_symbol
                or effective.token != "1d"
                or effective.intended_start != bounds["intended_start"]
                or effective.intended_end != bounds["intended_end"]):
            raise RequestRefused("StockAnalysis effective request changed before transport")
        if send is not None:
            raw = send(url)
        else:
            from fetch_http_transport import Deadline, build_opener
            from fetch_run_context import RequestCancelled
            from stock_validate import _UA

            response = None
            with Deadline(timeout) as deadline:
                primary = True
                try:
                    opener = build_opener(deadline)
                    wire = urllib.request.Request(url, headers=_UA, method="GET")
                    response = urllib.request.OpenerDirector.open(
                        opener, wire, timeout=deadline.remaining())
                    deadline.remaining()
                    body = bytearray()
                    while len(body) <= _MAX_HISTORY_BYTES:
                        if cancel is not None and cancel.is_set():
                            raise RequestCancelled("StockAnalysis body read cancelled")
                        deadline.remaining()
                        part = response.read1(min(64 * 1024, _MAX_HISTORY_BYTES + 1 - len(body)))
                        deadline.remaining()
                        if not isinstance(part, bytes):
                            raise RequestRefused("StockAnalysis response chunk is not bytes")
                        body.extend(part)
                        if not part:
                            break
                    raw = bytes(body)
                    primary = False
                except HTTPError as error:
                    response = error
                    deadline.remaining()
                    raise
                except OSError as error:
                    try:
                        deadline.remaining()
                    except TimeoutError as expired:
                        raise expired from error
                    raise
                finally:
                    if response is not None:
                        try:
                            response.close()
                        except BaseException:
                            if not primary:
                                raise
        if _capture is not None and isinstance(raw, bytes):
            _capture["raw_digest"] = hashlib.sha256(raw).hexdigest()
        return raw

    def decode(raw):
        if not isinstance(raw, bytes) or not raw or len(raw) > _MAX_HISTORY_BYTES:
            raise ResponseQuarantined("StockAnalysis raw response size is invalid")
        try:
            return strict_json(raw)
        except (ValueError, OverflowError, RecursionError) as exc:
            raise ResponseQuarantined(
                "StockAnalysis response is not strict JSON") from exc

    try:
        return request.execute(
            one_send, acquire_turn=lambda: 0,
            governor=governor, normalizer=decode,
            attempt_evidence=evidence, cancel=cancel)
    except HTTPError as error:
        # The executor has durably recorded the original result before this
        # StockAnalysis-specific terminal policy. SEC 403 semantics are separate.
        if error.code == 403:
            raise RequestRefused("StockAnalysis provider blocked the request (HTTP 403)") from error
        raise


def verified_stockanalysis_attempt_evidence(context, envelope, evidence):
    """Re-derive exact durable URL/range provenance from the captured context."""
    from fetch_run_context import RequestRefused

    if not isinstance(evidence, dict) or set(evidence) != {
            "url", "range", "requested_start_date", "truncated_coverage"}:
        raise RequestRefused("StockAnalysis attempt provenance is missing")
    rng = evidence["range"]
    if type(rng) is not str:
        raise RequestRefused("StockAnalysis range provenance is invalid")
    url = stockanalysis_url(envelope.subject, rng)
    bounds = stockanalysis_bounds(context.authority, context.captured_now, rng)
    expected = {
        "url": url, "range": rng,
        "requested_start_date": bounds["requested_start_date"],
        "truncated_coverage": bounds["truncated_coverage"],
    }
    if (evidence != expected or envelope.token != "1d"
            or envelope.intended_start != bounds["intended_start"]
            or envelope.intended_end != bounds["intended_end"]):
        raise RequestRefused("StockAnalysis attempt provenance changed")
    return expected


def stockanalysis_bounds(authority, captured_now: datetime, rng: str):
    """Resolve exact shipped 5Y/Max ranges within the captured authority."""
    captured_now = aware_ny(captured_now)
    if rng not in {"5Y", "Max"}:
        raise AuthorityError("unsupported StockAnalysis range")
    if rng == "Max":
        requested = authority.first_date
    else:
        today = captured_now.date()
        try:
            requested = today.replace(year=today.year - 5)
        except ValueError:
            requested = today.replace(year=today.year - 5, day=28)
    truncated = requested < authority.first_date
    day = max(requested, authority.first_date)
    start = None
    while day <= authority.valid_through:
        window = authority.window("1d", day)
        if window is not None:
            start = window[0]
            break
        day += timedelta(days=1)
    if start is None:
        raise AuthorityError("no covered open StockAnalysis start session")
    end = authority.horizon("1d", captured_now)
    if end is None or start >= end:
        raise AuthorityError("no settled StockAnalysis range")
    return {"range": rng, "requested_start_date": requested.isoformat(),
            "truncated_coverage": truncated, "intended_start": start,
            "intended_end": end}


def normalize_stockanalysis_rows(value):
    """Detach strict OHLCV rows while preserving each original date field."""
    try:
        return _normalize_stockanalysis_rows_strict(value)
    except (ValueError, OverflowError, RecursionError) as exc:
        raise ResponseQuarantined(str(exc)) from exc


def _normalize_stockanalysis_rows_strict(value):
    """Keep wire-data errors separate from authority and ledger failures."""
    # Import only after the request's transport guard has been established.
    from stock_validate import _norm_date_key

    rows = value.get("data") if isinstance(value, dict) else value
    if not isinstance(rows, list):
        raise AuthorityError("StockAnalysis response lacks a row list")
    if len(rows) > _MAX_HISTORY_ROWS:
        raise AuthorityError("StockAnalysis response has too many rows")
    result, seen = [], set()
    for raw in rows:
        if not isinstance(raw, dict) or "t" not in raw:
            raise AuthorityError("StockAnalysis row lacks a date field")
        source_date = raw["t"]
        if isinstance(source_date, bool) or not isinstance(
                source_date, (str, int, float)):
            raise AuthorityError("malformed StockAnalysis date field")
        # Retained wire evidence must be bounded too: a valid ISO fractional
        # tail can otherwise expand an under-cap body into an over-cap cache.
        # JSON's ASCII encoding accounts for quotes, escapes and numeric dates.
        if (isinstance(source_date, str) and len(source_date) >
                _MAX_SOURCE_DATE_JSON_BYTES) or len(json.dumps(
                    source_date, ensure_ascii=True, allow_nan=False)) > (
                        _MAX_SOURCE_DATE_JSON_BYTES):
            raise ResponseQuarantined("StockAnalysis source date exceeds the byte bound")
        if isinstance(source_date, str):
            if _DATE.match(source_date[:10]):
                try:
                    (date.fromisoformat(source_date) if len(source_date) == 10
                     else datetime.fromisoformat(source_date))
                except ValueError as exc:
                    raise AuthorityError("malformed StockAnalysis ISO date") from exc
            elif not re.fullmatch(r"(?:0|[1-9][0-9]*)", source_date):
                # The legacy parser below strips whitespace and accepts float
                # spellings. Only canonical ASCII epoch text may reach it here.
                raise AuthorityError("malformed StockAnalysis epoch text")
        label = _norm_date_key(source_date)
        if not isinstance(label, str) or not _DATE.fullmatch(label):
            raise AuthorityError("malformed StockAnalysis date label")
        try:
            parsed = date.fromisoformat(label)
        except ValueError as exc:
            raise AuthorityError("malformed StockAnalysis calendar date") from exc
        if parsed.isoformat() != label or label in seen:
            raise AuthorityError("duplicate or noncanonical StockAnalysis date")
        seen.add(label)
        numbers = {}
        for key in _OHLCV:
            try:
                number = float(raw[key])
            except (KeyError, TypeError, ValueError, OverflowError) as exc:
                raise AuthorityError("invalid StockAnalysis OHLCV row") from exc
            if not math.isfinite(number) or number < 0:
                raise AuthorityError("non-finite or negative StockAnalysis OHLCV")
            numbers[key] = number
        if (numbers["h"] < max(numbers["o"], numbers["c"])
                or numbers["l"] > min(numbers["o"], numbers["c"])):
            raise AuthorityError("invalid StockAnalysis OHLC ordering")
        result.append({"source_date": source_date, "label": label, **numbers})
    return result


def filter_stockanalysis_rows(rows, envelope, context):
    """Keep unsupported/future labels observed, never accepted or stamped."""
    accepted = []
    horizon = context.horizons["1d"]
    for row in rows:
        day = date.fromisoformat(row["label"])
        try:
            window = context.authority.window("1d", day)
            if window is None:
                continue
            close = window[1]
            if (horizon is None or close > horizon
                    or context.captured_now < context.authority.daily_settled_close(day)
                    or day < envelope.intended_start.date()
                    or day > envelope.intended_end.date()
                    or close > envelope.intended_end):
                continue
        except CalendarUnsupported:
            continue
        accepted.append({**row, "timestamp": close.astimezone(timezone.utc).isoformat()})
    return accepted
