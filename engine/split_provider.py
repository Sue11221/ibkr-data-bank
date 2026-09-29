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
import urllib.parse
import urllib.request


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
    from fetch_ibkr_bridge import refuse_a2
    refuse_a2()
    request = urllib.request.Request(url, headers=dict(headers or {}))
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read()


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
    """Bounded Yahoo/SEC provider with optional explicit CIK overrides."""

    provider_id = "yahoo-sec-companyfacts-v1"

    def __init__(self, fetcher=None, *, timeout=30, attempts=2,
                 retry_delay=0.25, sleep_fn=time.sleep, clock=None,
                 cik_overrides=None, sec_user_agent=None):
        self.fetcher = fetcher or _default_fetcher
        self.timeout = max(1, int(timeout))
        self.attempts = max(1, min(3, int(attempts)))
        self.retry_delay = max(0.0, float(retry_delay))
        self.sleep_fn = sleep_fn
        self.clock = clock
        self.cik_overrides = {
            str(ticker).strip().upper(): int(cik)
            for ticker, cik in dict(cik_overrides or {}).items()
        }
        if any(cik <= 0 for cik in self.cik_overrides.values()):
            raise ProviderError("CIK overrides must be positive integers")
        self.sec_user_agent = sec_user_agent or os.environ.get(
            "SEC_USER_AGENT",
            "EMA-Crossover-Momentum-Strategy/1.0 local-research")
        self._ticker_map = None
        self._ticker_map_source = None

    def _fetch(self, url, headers=None):
        from fetch_ibkr_bridge import refuse_a2
        refuse_a2()
        last = None
        for attempt in range(self.attempts):
            try:
                raw = self.fetcher(
                    url, headers=dict(headers or {}), timeout=self.timeout)
                if not isinstance(raw, (bytes, bytearray)):
                    raise TypeError("fetcher did not return bytes")
                return bytes(raw)
            except Exception as exc:  # noqa: BLE001 - normalized below
                last = exc
                if attempt + 1 < self.attempts and self.retry_delay:
                    self.sleep_fn(self.retry_delay)
        raise ProviderError(
            f"{url}: {type(last).__name__}: {last}") from last

    def _load_ticker_map(self):
        if self._ticker_map is not None:
            return self._ticker_map, self._ticker_map_source
        headers = {
            "User-Agent": self.sec_user_agent,
            "Accept-Encoding": "identity",
        }
        raw = self._fetch(SEC_TICKERS_URL, headers=headers)
        digest = _sha256(raw)
        mapping = parse_sec_tickers(_json_payload(raw, "SEC ticker map"))
        if not mapping:
            raise ProviderError("SEC ticker map contains no valid identities")
        source_id = f"sec-ticker-map:{digest}"
        source = {
            "source_id": source_id,
            "kind": "sec_ticker_map",
            "url": SEC_TICKERS_URL,
            "payload_sha256": digest,
            "bytes": len(raw),
        }
        self._ticker_map = mapping
        self._ticker_map_source = source
        return mapping, source

    def _resolve_cik(self, ticker, supplied_cik=None):
        if supplied_cik is not None:
            try:
                cik = int(supplied_cik)
            except (TypeError, ValueError) as exc:
                raise ProviderError(f"invalid CIK for {ticker}") from exc
            if cik <= 0:
                raise ProviderError(f"invalid CIK for {ticker}")
            return cik, {
                "source_id": f"configured-cik:{ticker}:{cik}",
                "kind": "configured_cik_binding",
                "ticker": ticker,
                "cik": cik,
            }
        if ticker in self.cik_overrides:
            cik = self.cik_overrides[ticker]
            return cik, {
                "source_id": f"configured-cik:{ticker}:{cik}",
                "kind": "configured_cik_binding",
                "ticker": ticker,
                "cik": cik,
            }
        mapping, source = self._load_ticker_map()
        cik = mapping.get(ticker)
        if cik is None:
            raise ProviderError(f"SEC ticker map has no CIK for {ticker}")
        return cik, source

    def fetch_history(self, ticker, provider_symbol=None, cik=None):
        """Fetch and normalize one ticker's split evidence."""
        ticker = str(ticker or "").strip().upper()
        provider_symbol = str(provider_symbol or ticker).strip().upper()
        if not ticker or not provider_symbol:
            raise ProviderError("ticker and provider symbol are required")
        now = _utc_now(self.clock)
        end_epoch = int((now + dt.timedelta(days=7)).timestamp())
        query = urllib.parse.urlencode({
            "period1": START_EPOCH,
            "period2": end_epoch,
            "interval": "1d",
            "events": "splits",
        })
        yahoo_url = (
            f"{YAHOO_URL.format(symbol=urllib.parse.quote(provider_symbol, safe=''))}"
            f"?{query}")
        yahoo_raw = self._fetch(
            yahoo_url, headers={"User-Agent": "Mozilla/5.0"})
        yahoo_digest = _sha256(yahoo_raw)
        yahoo_payload_id = f"yahoo-payload:{provider_symbol}:{yahoo_digest}"
        events, yahoo_records = parse_yahoo_events(
            _json_payload(yahoo_raw, f"Yahoo {provider_symbol}"),
            ticker, yahoo_payload_id)

        resolved_cik, cik_source = self._resolve_cik(ticker, supplied_cik=cik)
        sec_url = SEC_FACTS_URL.format(cik=resolved_cik)
        sec_raw = self._fetch(sec_url, headers={
            "User-Agent": self.sec_user_agent,
            "Accept-Encoding": "identity",
        })
        sec_digest = _sha256(sec_raw)
        sec_payload = _json_payload(sec_raw, f"SEC CompanyFacts {ticker}")
        try:
            payload_cik = int(sec_payload["cik"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ProviderError(
                f"SEC CompanyFacts {ticker}: missing numeric CIK") from exc
        if payload_cik != resolved_cik:
            raise ProviderError(
                f"SEC CompanyFacts {ticker}: CIK {payload_cik} != "
                f"{resolved_cik}")
        sec_payload_id = f"sec-companyfacts:{ticker}:{sec_digest}"
        facts, sec_records = parse_sec_split_facts(
            sec_payload, ticker, sec_payload_id)

        sources = {
            yahoo_payload_id: {
                "kind": "yahoo_chart_payload",
                "url": yahoo_url,
                "payload_sha256": yahoo_digest,
                "bytes": len(yahoo_raw),
            },
            sec_payload_id: {
                "kind": "sec_companyfacts_payload",
                "url": sec_url,
                "cik": resolved_cik,
                "payload_sha256": sec_digest,
                "bytes": len(sec_raw),
            },
            cik_source["source_id"]: cik_source,
            **yahoo_records,
            **sec_records,
        }

        issuer_record_id = None
        issuer_digest = None
        if ticker == "FTNT":
            issuer_raw = self._fetch(
                FTNT_ISSUER_URL, headers={"User-Agent": "Mozilla/5.0"})
            parse_ftnt_issuer_evidence(issuer_raw)
            _require_ftnt_complete_events(events)
            issuer_digest = _sha256(issuer_raw)
            issuer_record_id = (
                "issuer-record:FTNT:2022-second-stock-split-statement")
            sources[issuer_record_id] = {
                "kind": "issuer_complete_split_history",
                "url": FTNT_ISSUER_URL,
                "payload_sha256": issuer_digest,
                "record_id": issuer_record_id,
                "captured_at": now.isoformat(timespec="seconds"),
            }

        normalized_events = []
        for event in events:
            source_ids = list(event["source_ids"])
            support = _find_support(facts, event)
            if support is not None:
                source_ids.append(support["source_id"])
            if issuer_record_id and _ftnt_issuer_matches(event):
                source_ids.append(issuer_record_id)
            confirmed = len(source_ids) > len(event["source_ids"])
            normalized_events.append({
                **event,
                "confidence": "confirmed" if confirmed else "provisional",
                "source_ids": sorted(set(source_ids)),
            })

        if issuer_record_id:
            coverage = {
                "from": START_DATE.isoformat(),
                "through": now.date().isoformat(),
                "evidence_level": "issuer_explicit_history",
                "complete": True,
                "complete_basis": {
                    "kind": "issuer_explicit_history",
                    "source_url": FTNT_ISSUER_URL,
                    "payload_sha256": issuer_digest,
                    "captured_at": now.isoformat(timespec="seconds"),
                    "record_id": issuer_record_id,
                    "reason": (
                        "issuer identifies the 2022 event as its second split "
                        "and identifies the prior 2011 event"),
                    "source_ids": [issuer_record_id],
                },
                "source_ids": [issuer_record_id],
            }
        else:
            coverage = {
                "from": START_DATE.isoformat(),
                "through": now.date().isoformat(),
                "evidence_level": "provisional",
                "complete": False,
                "complete_basis": None,
                "source_ids": sorted({yahoo_payload_id, sec_payload_id}),
            }

        return {
            "ticker": ticker,
            "provider_symbol": provider_symbol,
            "cik": resolved_cik,
            "fetched_at": now.isoformat(timespec="seconds"),
            "provider": self.provider_id,
            "provider_status": "ok",
            "coverage": coverage,
            "events": normalized_events,
            "sources": sources,
        }
