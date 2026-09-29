"""Live Phase-0 proof for the split-history provider stack.

Yahoo chart events provide structured candidate dates. SEC CompanyFacts provides
authoritative split-ratio facts, while issuer evidence handles the rare case
where a negative history statement is required. Yahoo-only events are discovery
data and never sufficient for a severe verdict.

Run:
    python engine/split_provider_reference.py --json <artifact.json>
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import html
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

try:                                  # harness hygiene (Row 61): never die on a glyph
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:                     # noqa: BLE001 - older/redirected streams
    pass


YAHOO_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
SEC_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
SEC_FACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik:010d}.json"
FTNT_ISSUER_URL = (
    "https://www.fortinet.com/corporate/about-us/newsroom/press-releases/"
    "2022/fortinet-announces-five-for-one-stock-split")
SEC_SPLIT_TAG = "StockholdersEquityNoteStockSplitConversionRatio1"
START_EPOCH = 315532800  # 1980-01-01 UTC; Yahoo rejects period1=0 for some names.
SEC_PROOF_CIKS = {
    "FTNT": 1262039,
    "NVDA": 1045810,
    "GE": 40545,
}


def _sha256(payload):
    return hashlib.sha256(payload).hexdigest()


def _fetch_bytes(url, headers=None, timeout=30, attempts=2):
    headers = dict(headers or {})
    last = None
    for attempt in range(attempts):
        try:
            request = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return response.read()
        except Exception as exc:  # noqa: BLE001 - proof records exact source error
            last = exc
            if attempt + 1 < attempts:
                time.sleep(0.5)
    raise RuntimeError(f"{url}: {type(last).__name__}: {last}") from last


def _json_payload(raw, source):
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise ValueError(f"{source}: malformed JSON: {exc}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"{source}: top-level JSON is not an object")
    return value


def parse_yahoo_events(payload, ticker):
    try:
        result = payload["chart"]["result"][0]
    except (KeyError, IndexError, TypeError) as exc:
        raise ValueError(f"Yahoo {ticker}: missing chart result") from exc
    raw_events = (((result.get("events") or {}).get("splits")) or {})
    if not isinstance(raw_events, dict):
        raise ValueError(f"Yahoo {ticker}: split events are not an object")
    out = []
    for event in raw_events.values():
        try:
            numerator = float(event["numerator"])
            denominator = float(event["denominator"])
            stamp = int(event["date"])
            if numerator <= 0 or denominator <= 0:
                raise ValueError("non-positive ratio")
            day = dt.datetime.fromtimestamp(
                stamp, tz=dt.timezone.utc).date().isoformat()
        except (KeyError, TypeError, ValueError, OverflowError) as exc:
            raise ValueError(f"Yahoo {ticker}: malformed split row {event!r}") from exc
        out.append({
            "ticker": ticker,
            "ex_date": day,
            "numerator": numerator,
            "denominator": denominator,
            "ratio": numerator / denominator,
            "raw_ratio": event.get("splitRatio"),
            "source": "yahoo-chart-events",
            "confidence": "discovery",
        })
    return sorted(out, key=lambda row: row["ex_date"])


def fetch_yahoo_events(ticker, timeout=30):
    end_epoch = int((dt.datetime.now(dt.timezone.utc)
                     + dt.timedelta(days=7)).timestamp())
    query = urllib.parse.urlencode({
        "period1": START_EPOCH,
        "period2": end_epoch,
        "interval": "1d",
        "events": "splits",
    })
    url = f"{YAHOO_URL.format(ticker=ticker)}?{query}"
    raw = _fetch_bytes(url, headers={"User-Agent": "Mozilla/5.0"},
                       timeout=timeout)
    return parse_yahoo_events(_json_payload(raw, f"Yahoo {ticker}"), ticker), {
        "url": url,
        "sha256": _sha256(raw),
        "bytes": len(raw),
    }


def parse_sec_tickers(payload):
    out = {}
    for row in payload.values():
        if not isinstance(row, dict):
            continue
        ticker = str(row.get("ticker") or "").strip().upper()
        try:
            cik = int(row.get("cik_str"))
        except (TypeError, ValueError):
            continue
        if ticker:
            out[ticker] = cik
    return out


def parse_sec_split_facts(payload, ticker):
    facts = (((payload.get("facts") or {}).get("us-gaap") or {})
             .get(SEC_SPLIT_TAG) or {})
    rows = ((facts.get("units") or {}).get("pure") or [])
    out = []
    seen = set()
    for row in rows:
        try:
            ratio = float(row["val"])
            end = dt.date.fromisoformat(str(row["end"])[:10]).isoformat()
        except (KeyError, TypeError, ValueError):
            continue
        key = (ratio, end, str(row.get("accn") or ""))
        if key in seen or ratio <= 0:
            continue
        seen.add(key)
        out.append({
            "ticker": ticker,
            "fact_date": end,
            "ratio": ratio,
            "accession": row.get("accn"),
            "form": row.get("form"),
            "filed": row.get("filed"),
            "source": "sec-companyfacts",
            "confidence": "authoritative-ratio",
        })
    return sorted(out, key=lambda item: (item["fact_date"], item["ratio"]))


def fetch_sec_sources(tickers, timeout=30):
    user_agent = os.environ.get(
        "SEC_USER_AGENT",
        "EMA-Crossover-Momentum-Strategy/1.0 local-research")
    headers = {"User-Agent": user_agent, "Accept-Encoding": "identity"}
    # The SEC ticker-map route is intermittently blocked independently of
    # data.sec.gov. Keep it as corroborating lookup evidence, but bind these
    # fixed proof cases to their known CIKs and validate each returned payload.
    ticker_map_evidence = {"url": SEC_TICKERS_URL}
    try:
        ticker_raw = _fetch_bytes(
            SEC_TICKERS_URL, headers=headers, timeout=timeout, attempts=1)
    except RuntimeError as exc:
        ticker_map_evidence.update({
            "status": "unavailable",
            "error": str(exc),
        })
        mapping = dict(SEC_PROOF_CIKS)
    else:
        mapping = parse_sec_tickers(_json_payload(ticker_raw, "SEC tickers"))
        ticker_map_evidence.update({
            "status": "ok",
            "sha256": _sha256(ticker_raw),
            "bytes": len(ticker_raw),
        })
        for ticker, expected_cik in SEC_PROOF_CIKS.items():
            if mapping.get(ticker) != expected_cik:
                raise ValueError(
                    f"SEC ticker map CIK mismatch for {ticker}: "
                    f"{mapping.get(ticker)!r} != {expected_cik}")
    facts = {}
    evidence = {
        "ticker_map": ticker_map_evidence,
        "proof_cik_bindings": dict(SEC_PROOF_CIKS),
        "companyfacts": {},
    }
    for ticker in tickers:
        cik = mapping.get(ticker)
        if cik is None:
            raise ValueError(f"SEC ticker map has no CIK for {ticker}")
        url = SEC_FACTS_URL.format(cik=cik)
        raw = _fetch_bytes(url, headers=headers, timeout=timeout)
        payload = _json_payload(raw, f"SEC CompanyFacts {ticker}")
        try:
            payload_cik = int(payload["cik"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(
                f"SEC CompanyFacts {ticker}: missing numeric CIK") from exc
        if payload_cik != cik:
            raise ValueError(
                f"SEC CompanyFacts {ticker}: CIK {payload_cik} != {cik}")
        facts[ticker] = parse_sec_split_facts(payload, ticker)
        evidence["companyfacts"][ticker] = {
            "url": url,
            "cik": cik,
            "sha256": _sha256(raw),
            "bytes": len(raw),
        }
    return facts, evidence


def fetch_ftnt_issuer_evidence(timeout=30):
    raw = _fetch_bytes(
        FTNT_ISSUER_URL,
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=timeout)
    text = html.unescape(re.sub(r"<[^>]+>", " ", raw.decode(
        "utf-8", "replace")))
    text = re.sub(r"\s+", " ", text).lower()
    return {
        "url": FTNT_ISSUER_URL,
        "sha256": _sha256(raw),
        "bytes": len(raw),
        "states_second_split": "second time" in text,
        "states_2011_two_for_one": (
            "2011" in text and "two-for-one stock split" in text),
    }


def _find_event(events, day, ratio, date_tolerance=1, ratio_tolerance=1e-9):
    wanted_day = dt.date.fromisoformat(day)
    for event in events:
        current = dt.date.fromisoformat(event["ex_date"])
        if (abs((current - wanted_day).days) <= date_tolerance
                and abs(float(event["ratio"]) - ratio) <= ratio_tolerance):
            return event
    return None


def _sec_supports(facts, event, max_days=183, ratio_tolerance=1e-9):
    event_day = dt.date.fromisoformat(event["ex_date"])
    for fact in facts:
        fact_day = dt.date.fromisoformat(fact["fact_date"])
        if (abs((event_day - fact_day).days) <= max_days
                and abs(float(event["ratio"]) - float(fact["ratio"]))
                <= ratio_tolerance):
            return fact
    return None


def _check(checks, name, condition, detail):
    checks.append({
        "name": name,
        "pass": bool(condition),
        "detail": detail,
    })


def run_proof(timeout=30):
    tickers = ("FTNT", "NVDA", "GE")
    result = {
        "kind": "split_provider_proof",
        "version": 1,
        "asof": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "network": True,
        "bank_writes": False,
        "checks": [],
        "sources": {},
        "events": {},
        "sec_facts": {},
    }
    checks = result["checks"]
    yahoo = {}
    yahoo_evidence = {}
    for ticker in tickers:
        yahoo[ticker], yahoo_evidence[ticker] = fetch_yahoo_events(
            ticker, timeout=timeout)
    sec_facts, sec_evidence = fetch_sec_sources(tickers, timeout=timeout)
    issuer = fetch_ftnt_issuer_evidence(timeout=timeout)
    result["events"] = yahoo
    result["sec_facts"] = sec_facts
    result["sources"] = {
        "yahoo": yahoo_evidence,
        "sec": sec_evidence,
        "ftnt_issuer": issuer,
    }

    targets = [
        ("FTNT", "2011-06-02", 2.0),
        ("FTNT", "2022-06-23", 5.0),
        ("NVDA", "2021-07-20", 4.0),
        ("NVDA", "2024-06-10", 10.0),
        ("GE", "2021-08-02", 0.125),
    ]
    matched = {}
    for ticker, day, ratio in targets:
        event = _find_event(yahoo[ticker], day, ratio)
        key = f"{ticker}:{day}:{ratio:g}"
        matched[key] = event
        _check(checks, f"Yahoo discovers {key}", event is not None,
               event or "missing")

    for ticker, day, ratio in targets[1:]:
        key = f"{ticker}:{day}:{ratio:g}"
        event = matched[key]
        support = _sec_supports(sec_facts[ticker], event) if event else None
        _check(checks, f"SEC ratio corroborates {key}", support is not None,
               support or "no SEC ratio fact within 183 days")

    ftnt_2014 = [event for event in yahoo["FTNT"]
                 if event["ex_date"].startswith("2014-")]
    _check(checks, "Yahoo has no FTNT 2014 event", not ftnt_2014,
           ftnt_2014 or "none")
    _check(checks, "Issuer states 2022 was FTNT's second split",
           issuer["states_second_split"], issuer)
    _check(checks, "Issuer identifies FTNT's prior 2011 two-for-one split",
           issuer["states_2011_two_for_one"], issuer)

    unsupported_ge = []
    for day, ratio in (("2023-01-04", 1.281), ("2024-04-02", 1.253)):
        event = _find_event(yahoo["GE"], day, ratio)
        if event and _sec_supports(sec_facts["GE"], event) is None:
            unsupported_ge.append(event)
    _check(checks, "Yahoo exposes non-SEC GE adjustment events",
           len(unsupported_ge) == 2, unsupported_ge)

    try:
        parse_yahoo_events({}, "BROKEN")
    except ValueError as exc:
        malformed_safe = True
        malformed_detail = str(exc)
    else:
        malformed_safe = False
        malformed_detail = "malformed payload was accepted"
    _check(checks, "Malformed provider payload fails closed",
           malformed_safe, malformed_detail)

    result["provider_decision"] = {
        "discovery": "Yahoo chart split events (structured, provisional)",
        "authoritative_confirmation": (
            "SEC CompanyFacts StockholdersEquityNoteStockSplitConversionRatio1"),
        "negative_confirmation": (
            "issuer/filing evidence or a separately configured complete feed"),
        "policy": (
            "Yahoo-only events cannot support PHANTOM or MISSING; SEC-confirmed "
            "events can support REAL/MISSING; negative PHANTOM decisions require "
            "explicit complete or issuer evidence"),
        "known_limitation": (
            "Yahoo mixes some spinoff/share-adjustment factors into split events; "
            "the SEC ticker-map route may be unavailable even when direct "
            "CompanyFacts works, so production refresh needs an explicit "
            "identity mapping and must fail closed when it has none"),
    }
    result["result"] = "pass" if all(row["pass"] for row in checks) else "fail"
    return result


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--json")
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args(sys.argv[1:] if argv is None else argv)
    try:
        result = run_proof(timeout=args.timeout)
    except Exception as exc:  # noqa: BLE001 - artifact must capture live failure
        result = {
            "kind": "split_provider_proof",
            "version": 1,
            "asof": dt.datetime.now(dt.timezone.utc).isoformat(
                timespec="seconds"),
            "network": True,
            "bank_writes": False,
            "result": "fail",
            "error": f"{type(exc).__name__}: {exc}",
        }
    payload = json.dumps(result, indent=2, sort_keys=True)
    if args.json:
        Path(args.json).write_text(payload + "\n", encoding="utf-8")
    print(payload)
    return 0 if result.get("result") == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
