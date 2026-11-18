"""Explicit split-history provider adapter.

Yahoo chart events supply candidate ex-dates and ratios. SEC CompanyFacts can
corroborate a ratio, but does not establish an exact ex-date or complete
history. Fortinet's issuer statement is the one proven complete-history source
implemented here. Network access occurs only when ``fetch_history`` is called;
the fetcher is injectable for deterministic offline tests.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import html
import json
import os
import re
import time


YAHOO_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
SEC_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
SEC_FACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik:010d}.json"
FTNT_ISSUER_URL = (
    "https://www.fortinet.com/corporate/about-us/newsroom/press-releases/"
    "2022/fortinet-announces-five-for-one-stock-split")
SEC_SPLIT_TAG = "StockholdersEquityNoteStockSplitConversionRatio1"
START_DATE = dt.date(1980, 1, 1)
START_EPOCH = 315532800
SEC_MATCH_DAYS = 183
RATIO_TOL = 1e-9


class ProviderError(RuntimeError):
    """Provider data was unavailable, malformed, or identity-unsafe."""


def _sha256(payload):
    return hashlib.sha256(payload).hexdigest()


def _json_payload(raw, source):
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise ProviderError(f"{source}: malformed JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise ProviderError(f"{source}: top-level JSON is not an object")
    return payload


def _positive(value, label):
    if isinstance(value, bool):
        raise ProviderError(f"invalid {label}: {value!r}")
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ProviderError(f"invalid {label}: {value!r}") from exc
    if number <= 0 or number != number or number in (float("inf"), float("-inf")):
        raise ProviderError(f"invalid {label}: {value!r}")
    return number


def _utc_now(clock=None):
    value = clock() if clock is not None else dt.datetime.now(dt.timezone.utc)
    if isinstance(value, dt.date) and not isinstance(value, dt.datetime):
        value = dt.datetime.combine(value, dt.time(), tzinfo=dt.timezone.utc)
    if not isinstance(value, dt.datetime):
        raise ProviderError("clock did not return a date or datetime")
    if value.tzinfo is None:
        value = value.replace(tzinfo=dt.timezone.utc)
    return value.astimezone(dt.timezone.utc)


def _default_fetcher(url, headers=None, timeout=30):
    """Refusal-only compatibility entry; the legacy physical sender is retired."""
    from fetch_ibkr_bridge import refuse_a2
    refuse_a2()


def parse_yahoo_events(payload, ticker, payload_source_id):
    """Parse Yahoo split rows as provisional exact-date events."""
    try:
        result = payload["chart"]["result"][0]
    except (KeyError, IndexError, TypeError) as exc:
        raise ProviderError(f"Yahoo {ticker}: missing chart result") from exc
    raw_events = (((result.get("events") or {}).get("splits")) or {})
    if not isinstance(raw_events, dict):
        raise ProviderError(f"Yahoo {ticker}: split events are not an object")
    events = []
    records = {}
    for key, raw in sorted(raw_events.items(), key=lambda item: str(item[0])):
        if not isinstance(raw, dict):
            raise ProviderError(f"Yahoo {ticker}: split row is not an object")
        try:
            numerator = _positive(raw.get("numerator"), "Yahoo numerator")
            denominator = _positive(raw.get("denominator"), "Yahoo denominator")
            stamp = int(raw["date"])
            day = dt.datetime.fromtimestamp(
                stamp, tz=dt.timezone.utc).date().isoformat()
        except (KeyError, TypeError, ValueError, OverflowError) as exc:
            raise ProviderError(
                f"Yahoo {ticker}: malformed split row {raw!r}") from exc
        ratio = numerator / denominator
        source_id = (
            f"yahoo-event:{ticker}:{day}:{numerator:g}/{denominator:g}")
        events.append({
            "ex_date": day,
            "ratio": ratio,
            "ratio_convention": "new_shares_per_old_share",
            "confidence": "provisional",
            "source_ids": [source_id],
        })
        records[source_id] = {
            "kind": "yahoo_split_event",
            "payload_source_id": payload_source_id,
            "record_id": str(key),
            "ex_date": day,
            "ratio": ratio,
        }
    events.sort(key=lambda row: (row["ex_date"], row["ratio"]))
    return events, records


def parse_sec_tickers(payload):
    mapping = {}
    for row in payload.values():
        if not isinstance(row, dict):
            continue
        ticker = str(row.get("ticker") or "").strip().upper()
        try:
            cik = int(row.get("cik_str"))
        except (TypeError, ValueError):
            continue
        if ticker and cik > 0:
            mapping[ticker] = cik
    return mapping


def parse_sec_split_facts(payload, ticker, payload_source_id):
    facts = (((payload.get("facts") or {}).get("us-gaap") or {})
             .get(SEC_SPLIT_TAG) or {})
    rows = ((facts.get("units") or {}).get("pure") or [])
    if not isinstance(rows, list):
        raise ProviderError(f"SEC CompanyFacts {ticker}: units are not a list")
    out = []
    records = {}
    seen = set()
    for raw in rows:
        if not isinstance(raw, dict):
            continue
        try:
            ratio = _positive(raw.get("val"), "SEC split ratio")
            fact_date = dt.date.fromisoformat(
                str(raw.get("end") or "")[:10]).isoformat()
        except (ProviderError, ValueError):
            continue
        accession = str(raw.get("accn") or "").strip()
        key = (ratio, fact_date, accession)
        if key in seen:
            continue
        seen.add(key)
        record_part = accession or f"{fact_date}:{ratio:g}"
        source_id = f"sec-split-fact:{ticker}:{record_part}"
        row = {
            "fact_date": fact_date,
            "ratio": ratio,
            "accession": accession or None,
            "form": raw.get("form"),
            "filed": raw.get("filed"),
            "source_id": source_id,
        }
        out.append(row)
        records[source_id] = {
            "kind": "sec_companyfacts_split_ratio",
            "payload_source_id": payload_source_id,
            **row,
        }
    out.sort(key=lambda row: (row["fact_date"], row["ratio"],
                              row["source_id"]))
    return out, records


def parse_ftnt_issuer_evidence(raw):
    text = html.unescape(re.sub(
        r"<[^>]+>", " ", raw.decode("utf-8", "replace")))
    normalized = re.sub(r"\s+", " ", text).strip().lower()
    second = "second time" in normalized
    prior = "2011" in normalized and "two-for-one stock split" in normalized
    if not second or not prior:
        raise ProviderError(
            "FTNT issuer completeness evidence changed or is unparseable")
    return {
        "states_second_split": True,
        "states_2011_two_for_one": True,
    }


def _find_support(facts, event):
    event_day = dt.date.fromisoformat(event["ex_date"])
    for fact in facts:
        fact_day = dt.date.fromisoformat(fact["fact_date"])
        if (abs((event_day - fact_day).days) <= SEC_MATCH_DAYS
                and abs(float(event["ratio"]) - float(fact["ratio"]))
                <= RATIO_TOL):
            return fact
    return None


def _ftnt_issuer_matches(event):
    return (
        event["ex_date"] == "2011-06-02" and abs(event["ratio"] - 2.0) <= RATIO_TOL
    ) or (
        event["ex_date"] == "2022-06-23" and abs(event["ratio"] - 5.0) <= RATIO_TOL
    )


def _require_ftnt_complete_events(events):
    expected = {
        ("2011-06-02", 2.0),
        ("2022-06-23", 5.0),
    }
    actual = {
        (event["ex_date"], float(event["ratio"]))
        for event in events
        if _ftnt_issuer_matches(event)
    }
    if actual != expected:
        raise ProviderError(
            "FTNT issuer evidence and discovered event history do not agree")


class YahooSecProvider:
    """Compose accepted guarded evidence; no root or reusable authority."""

    provider_id = "yahoo-sec-companyfacts-v1"
    MAP_TTL_SECONDS = 3600

    def __init__(self, fetcher=None, *, timeout=30, attempts=2,
                 retry_delay=0.25, sleep_fn=time.sleep, clock=None,
                 cik_overrides=None, sec_user_agent=None):
        from fetch_http_splits import _metadata_cik, _symbol
        if type(timeout) not in (int, float) or not 0 < timeout <= 120:
            raise ProviderError("invalid split timeout")
        if type(attempts) is not int or not 1 <= attempts <= 3:
            raise ProviderError("invalid split attempt cap")
        if cik_overrides is not None and type(cik_overrides) is not dict:
            raise ProviderError("CIK overrides must be a detached dictionary")
        self.fetcher = fetcher
        self.timeout = timeout
        self.attempts = attempts
        self.sec_user_agent = sec_user_agent
        self.cik_overrides = {}
        try:
            for symbol, cik in (cik_overrides or {}).items():
                self.cik_overrides[_symbol(symbol)] = _metadata_cik(cik)
        except ValueError as error:
            raise ProviderError("invalid configured CIK") from error
        # Legacy clock/sleep arguments are not permission sources and are unused.
        self._pending_map = None
        self._pending_map_path = None
        self._sealed_map = None
        self._sealed_map_path = None

    def _fetch(self, url, headers=None):
        """Retained legacy helper is held; no local retries or raw fallback."""
        from fetch_ibkr_bridge import refuse_a2
        refuse_a2()

    @staticmethod
    def _admitted_worker(worker):
        from fetch_operations import check_worker
        from fetch_run_context import FetchWorker, RequestRefused
        if type(worker) is not FetchWorker:
            raise RequestRefused("split history requires an explicit operation worker")
        check_worker(worker.context, worker.worker_id)
        worker.context.ledger.require_live()
        return worker.context

    def _attempt(self, worker, producer, symbol, *, cik=None, cancel=None):
        from fetch_http_splits import guarded_split_fetch
        capture = {}
        provenance = {}
        options = dict(timeout=self.timeout, cancel=cancel, _capture=capture,
                       _provenance=provenance)
        if producer.endswith("sec_tickers") or producer.endswith("sec_facts"):
            options["sec_user_agent"] = self.sec_user_agent
        if cik is not None:
            options["cik"] = cik
        send = None
        if self.fetcher is not None:
            def send(url):
                # The executor clears authority before this test-only callback.
                headers = {"User-Agent": self.sec_user_agent,
                           "Accept-Encoding": "identity"} if ".sec_" in producer else {}
                return self.fetcher(url, headers=headers, timeout=self.timeout)
        value = guarded_split_fetch(worker, producer, symbol, send,
                                    attempts=self.attempts, **options)
        capture.update(provenance)
        return value, capture

    @staticmethod
    def _map_value_matches(entry, context):
        from fetch_authority import digest_value
        from fetch_http_splits import _metadata_cik, _symbol, _EXEMPTIONS
        if type(entry) is not dict or entry.get("kind") != "guarded_sec_ticker_map":
            return False
        rows = entry.get("rows")
        provenance = entry.get("request_provenance")
        if (type(rows) is not dict or not 1 <= len(rows) <= 20000
                or type(provenance) is not dict
                or provenance.get("producer") != "http.split_provider.sec_tickers"
                or provenance.get("endpoint") != "sec.tickers"
                or provenance.get("subject") != "SEC:TICKERS"
                or provenance.get("url") != SEC_TICKERS_URL
                or provenance.get("metadata_exemption") != _EXEMPTIONS["sec.tickers"]
                or entry.get("authority_fingerprint") != context.authority.fingerprint
                or entry.get("table_digest") != context.authority.table_digest
                or entry.get("accepted_digest") != digest_value(rows)):
            return False
        try:
            for symbol, cik in rows.items():
                _symbol(symbol)
                if type(cik) is not int or _metadata_cik(cik) != cik:
                    return False
            stamp = dt.datetime.fromisoformat(entry["captured_at"])
            age = (context.captured_now - stamp).total_seconds()
            return 0 <= age <= YahooSecProvider.MAP_TTL_SECONDS
        except (KeyError, TypeError, ValueError):
            return False

    @staticmethod
    def _map_result_identity(entry, audit):
        from fetch_authority import digest_value
        from fetch_run_context import RequestRefused
        if not audit["verified"] or audit["operation_id"] != entry["operation_id"]:
            raise RequestRefused("ticker-map source operation is not sealed")
        decisions = {event["attempt_id"]: event for event in audit["events"]
                     if event["event"] == "decision"}
        matches = []
        for event in audit["events"]:
            if (event["event"] != "result"
                    or event["producer_id"] != "http.split_provider.sec_tickers"
                    or event["logical_id"] != entry["logical_id"]):
                continue
            result = event["payload"]
            decision = decisions[event["attempt_id"]]["payload"]
            if (result.get("outcome") == "returned"
                    and result.get("raw_response_digest") == entry["raw_digest"]
                    and result.get("response_digest") == entry["accepted_digest"]
                    and result.get("metadata_rows", {}).get("accepted_count") == len(entry["rows"])
                    and decision.get("http_request") == entry["request_provenance"]
                    and decision.get("authority_fingerprint") == entry["authority_fingerprint"]
                    and decision.get("table_digest") == entry["table_digest"]
                    and decision.get("captured_now") == entry["captured_at"]):
                matches.append(event)
        if len(matches) != 1:
            raise RequestRefused("ticker-map source result does not match")
        seal = audit["events"][-1]
        if seal["event"] != "seal":
            raise RequestRefused("ticker-map source seal is missing")
        return dict(attempt_id=matches[0]["attempt_id"], result_seq=matches[0]["seq"],
                    seal_seq=seal["seq"], seal_digest=digest_value(seal))

    def _publish_ticker_map(self, context):
        """Fixed owner publisher AFTER join/seal; no callback or root mint."""
        from copy import deepcopy
        from fetch_ledger import inspect_ledger
        from fetch_run_context import RequestRefused
        entry = self._pending_map
        if entry is None:
            return
        if (entry["operation_id"] != context.operation_id
                or not self._map_value_matches(entry, context)
                or self._pending_map_path != context.ledger.path):
            raise RequestRefused("pending ticker-map publication identity differs")
        audit = inspect_ledger(context.ledger.path)
        identity = self._map_result_identity(entry, audit)
        self._sealed_map = deepcopy(dict(entry, sealed_identity=identity))
        self._sealed_map_path = context.ledger.path
        self._pending_map = None
        self._pending_map_path = None

    def _load_ticker_map(self, worker, symbol, *, cancel=None):
        from copy import deepcopy
        from fetch_authority import AuthorityError
        from fetch_http_splits import split_request
        from fetch_ledger import LedgerError, inspect_ledger
        from fetch_run_context import RequestRefused
        context = self._admitted_worker(worker)
        # Admission applies equally on pending and sealed cache paths.
        request = split_request(worker, "http.split_provider.sec_tickers", symbol,
                                sec_user_agent=self.sec_user_agent)
        if not context.permits(request.request.producer_id, request.request.envelope):
            raise RequestRefused("ticker-map cache admission refused")
        entry = None
        if (self._pending_map is not None
                and self._pending_map.get("operation_id") == context.operation_id
                and self._pending_map_path == context.ledger.path
                and self._map_value_matches(self._pending_map, context)):
            entry = self._pending_map
        elif self._sealed_map is not None and self._map_value_matches(self._sealed_map, context):
            try:
                identity = self._map_result_identity(self._sealed_map,
                                                    inspect_ledger(self._sealed_map_path))
                if identity == self._sealed_map.get("sealed_identity"):
                    entry = self._sealed_map
            except (AuthorityError, LedgerError, OSError, KeyError, TypeError, ValueError):
                pass  # A cache miss can only take a NEW guarded map request.
        if entry is None:
            rows, capture = self._attempt(worker, "http.split_provider.sec_tickers",
                                          symbol, cancel=cancel)
            entry = dict(kind="guarded_sec_ticker_map", rows=deepcopy(rows),
                         authority_fingerprint=context.authority.fingerprint,
                         table_digest=context.authority.table_digest,
                         captured_at=context.captured_now.isoformat(), **capture)
            self._pending_map = deepcopy(entry)
            self._pending_map_path = context.ledger.path
        source = dict(source_id="sec-ticker-map:" + entry["raw_digest"],
                      kind="sec_ticker_map", url=SEC_TICKERS_URL,
                      payload_sha256=entry["raw_digest"], bytes=entry["observed_bytes"],
                      provenance={key: deepcopy(entry[key]) for key in (
                          "operation_id", "logical_id", "accepted_digest",
                          "authority_fingerprint", "table_digest", "request_provenance")})
        if "sealed_identity" in entry:
            source["provenance"]["sealed_identity"] = deepcopy(entry["sealed_identity"])
        return deepcopy(entry["rows"]), source

    def _resolve_cik(self, worker, ticker, symbol, supplied_cik=None, *, cancel=None):
        from fetch_http_splits import _metadata_cik
        value = supplied_cik if supplied_cik is not None else self.cik_overrides.get(ticker)
        if value is not None:
            try:
                cik = _metadata_cik(value)
            except ValueError as error:
                raise ProviderError("invalid configured CIK") from error
            return cik, dict(source_id=f"configured-cik:{ticker}:{cik}",
                             kind="configured_cik_binding", ticker=ticker, cik=cik)
        mapping, source = self._load_ticker_map(worker, symbol, cancel=cancel)
        cik = mapping.get(symbol)
        if cik is None:
            raise ProviderError("SEC ticker map has no CIK for requested symbol")
        return cik, source

    def fetch_history(self, ticker, provider_symbol=None, cik=None, *,
                      _fetch_worker=None, _cancel=None):
        """Use ONLY durable accepted evidence from the explicit child's guard."""
        from copy import deepcopy
        from fetch_http_splits import _symbol, validate_sec_user_agent, yahoo_bounds
        context = self._admitted_worker(_fetch_worker)
        validate_sec_user_agent(self.sec_user_agent)
        ticker = _symbol(ticker)
        symbol = _symbol(provider_symbol if provider_symbol is not None else ticker)
        start, end = yahoo_bounds(context)
        yahoo_rows, yahoo = self._attempt(_fetch_worker, "http.split_provider.yahoo",
                                          symbol, cancel=_cancel)
        resolved_cik, cik_source = self._resolve_cik(_fetch_worker, ticker, symbol,
                                                    cik, cancel=_cancel)
        sec, sec_evidence = self._attempt(_fetch_worker, "http.split_provider.sec_facts",
                                         symbol, cik=resolved_cik, cancel=_cancel)
        yahoo_payload_id = f"yahoo-payload:{symbol}:{yahoo['raw_digest']}"
        sec_payload_id = f"sec-companyfacts:{ticker}:{sec_evidence['raw_digest']}"

        def payload_source(kind, evidence, **extra):
            return dict(kind=kind, url=evidence["request_provenance"]["url"],
                        payload_sha256=evidence["raw_digest"],
                        bytes=evidence["observed_bytes"],
                        provenance={key: deepcopy(evidence[key]) for key in (
                            "operation_id", "logical_id", "accepted_digest", "request_provenance")},
                        **extra)

        sources = {yahoo_payload_id: payload_source("yahoo_chart_payload", yahoo),
                   sec_payload_id: payload_source("sec_companyfacts_payload", sec_evidence,
                                                   cik=resolved_cik),
                   cik_source["source_id"]: cik_source}
        events = []
        for row in yahoo_rows:
            source_id = f"yahoo-event:{ticker}:{row['ex_date']}:{row['record_id']}"
            events.append(dict(ex_date=row["ex_date"], ratio=row["ratio"],
                               ratio_convention=row["ratio_convention"],
                               confidence="provisional", source_ids=[source_id]))
            sources[source_id] = dict(kind="yahoo_split_event",
                payload_source_id=yahoo_payload_id, record_id=row["record_id"],
                source_epoch=row["source_epoch"], ex_date=row["ex_date"], ratio=row["ratio"])
        sources.update({source_id: dict(deepcopy(record), payload_source_id=sec_payload_id)
                        for source_id, record in sec["records"].items()})
        facts = sec["split_facts"]
        issuer_record_id = None
        issuer = None
        now = context.captured_now
        if ticker == "FTNT":
            issuer_value, issuer = self._attempt(_fetch_worker,
                "http.split_provider.issuer", "FTNT", cancel=_cancel)
            if issuer_value != {"states_second_split": True, "states_2011_two_for_one": True}:
                raise ProviderError("FTNT issuer completeness evidence is not accepted")
            _require_ftnt_complete_events(events)
            issuer_record_id = "issuer-record:FTNT:2022-second-stock-split-statement"
            sources[issuer_record_id] = dict(kind="issuer_complete_split_history",
                url=FTNT_ISSUER_URL, payload_sha256=issuer["raw_digest"],
                record_id=issuer_record_id, captured_at=now.isoformat(timespec="seconds"),
                provenance=payload_source("fortinet_history", issuer)["provenance"])
        normalized_events = []
        for event in events:
            source_ids = list(event["source_ids"])
            support = _find_support(facts, event)
            if support is not None:
                source_ids.append(support["source_id"])
            if issuer_record_id and _ftnt_issuer_matches(event):
                source_ids.append(issuer_record_id)
            normalized_events.append(dict(event,
                confidence="confirmed" if len(source_ids) > len(event["source_ids"]) else "provisional",
                source_ids=sorted(set(source_ids))))
        normalized_events.sort(key=lambda row: (row["ex_date"], row["ratio"]))
        coverage = dict(
            **{"from": start.date().isoformat()}, through=end.date().isoformat(),
            evidence_level="issuer_explicit_history" if issuer_record_id else "provisional",
            complete=bool(issuer_record_id), complete_basis=None,
            source_ids=[issuer_record_id] if issuer_record_id else sorted((yahoo_payload_id, sec_payload_id)))
        if issuer_record_id:
            coverage["complete_basis"] = dict(kind="issuer_explicit_history",
                source_url=FTNT_ISSUER_URL, payload_sha256=issuer["raw_digest"],
                captured_at=now.isoformat(timespec="seconds"), record_id=issuer_record_id,
                reason="issuer identifies the 2022 event as its second split and the prior 2011 event",
                source_ids=[issuer_record_id])
        return deepcopy(dict(ticker=ticker, provider_symbol=symbol, cik=resolved_cik,
            fetched_at=now.isoformat(timespec="seconds"), provider=self.provider_id,
            provider_status="ok", coverage=coverage, events=normalized_events, sources=sources))
