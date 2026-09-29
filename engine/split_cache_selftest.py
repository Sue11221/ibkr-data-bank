"""Offline self-tests for split_provider.py and split_cache.py.

All provider bytes are injected fixtures. Filesystem writes are confined to a
temporary storage root.
"""

import contextlib
import datetime as dt
import io
import json
import sys
import tempfile
import threading
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
import split_cache as cache  # noqa: E402
import split_provider as provider_mod  # noqa: E402
import stock_storage as storage  # noqa: E402


FAILS = []
N = [0]
FIXED_NOW = dt.datetime(2026, 7, 9, 12, 0, tzinfo=dt.timezone.utc)


def check(name, condition, detail=""):
    N[0] += 1
    print(f"[{'PASS' if condition else 'FAIL'}] {name}"
          + (f" -- {detail}" if detail and not condition else ""))
    if not condition:
        FAILS.append(name)


def seed_manifest(root, ticker, conid, symbol=None):
    ticker_dir = Path(root) / ticker
    ticker_dir.mkdir(parents=True, exist_ok=True)
    manifest = storage.new_manifest(symbol or ticker, ticker)
    manifest["conid"] = conid
    storage.save_manifest(ticker_dir, manifest)
    return ticker_dir


def seed_daily_ratio_series(root, ticker, conid, ratios):
    ticker_dir = seed_manifest(root, ticker, conid)
    manifest = storage.load_manifest(ticker_dir)
    grouped = {}
    reference = {}
    day = dt.date(2010, 1, 1)
    index = 0
    while index < len(ratios):
        if day.weekday() < 5:
            stamp = dt.datetime.combine(day, dt.time())
            close = 100.0 * float(ratios[index])
            grouped.setdefault(day.isoformat()[:7], []).append(
                (stamp, close, close, close, close, 1000))
            reference[day.isoformat()] = (
                100.0, 100.0, 100.0, 100.0, 1000)
            index += 1
        day += dt.timedelta(days=1)
    months = storage.manifest_months(manifest, "1d")
    for month, bars in grouped.items():
        year, number = map(int, month.split("-"))
        path = storage.month_file_path(
            root, ticker, year, number, "1d")
        months[month] = storage.write_month_file(path, bars)
    storage.save_manifest(ticker_dir, manifest)
    return reference


def provisional_coverage(source_id="fixture-source"):
    return {
        "from": "1980-01-01",
        "through": "2026-07-09",
        "evidence_level": "provisional",
        "complete": False,
        "complete_basis": None,
        "source_ids": [source_id],
    }


def provider_result(ticker, symbol=None, *, fetched_at=None, cik=123456,
                    events=None, coverage=None, marker=None):
    source_id = "fixture-source"
    result = {
        "ticker": ticker,
        "provider_symbol": symbol or ticker,
        "cik": cik,
        "fetched_at": fetched_at or FIXED_NOW.isoformat(timespec="seconds"),
        "provider": "fixture-provider-v1",
        "provider_status": "ok",
        "coverage": coverage or provisional_coverage(source_id),
        "events": list(events or [{
            "ex_date": "2024-06-10",
            "ratio": 10.0,
            "confidence": "confirmed",
            "source_ids": [source_id],
        }]),
        "sources": {
            source_id: {
                "kind": "fixture",
                "payload_sha256": "f" * 64,
            },
        },
    }
    if marker is not None:
        result["marker"] = marker
    return result


class StaticProvider:
    def __init__(self, result_fn=None, error=None):
        self.result_fn = result_fn or (
            lambda ticker, symbol, cik: provider_result(
                ticker, symbol, cik=cik or 123456))
        self.error = error
        self.calls = []

    def fetch_history(self, ticker, provider_symbol=None, cik=None):
        self.calls.append((ticker, provider_symbol, cik))
        if self.error is not None:
            raise self.error
        return self.result_fn(ticker, provider_symbol, cik)


FIXTURE_FINGERPRINT = "a" * 64
FIXTURE_REFERENCE = {
    "2026-07-08": (100.0, 101.0, 99.0, 100.0, 1000),
}


def fixture_reference(_symbol, rng="Max"):
    assert rng == "Max"
    return FIXTURE_REFERENCE


def clean_scan(_root, ticker, ref=None):
    assert ref is FIXTURE_REFERENCE
    return {
        "ticker": ticker,
        "verdict": "CLEAN",
        "candidates": [],
        "series_fingerprint": FIXTURE_FINGERPRINT,
    }


PHASE3_FIXTURES = {
    "reference_fetcher": fixture_reference,
    "scan_fn": clean_scan,
}


def epoch(day):
    parsed = dt.datetime.combine(
        dt.date.fromisoformat(day), dt.time(), tzinfo=dt.timezone.utc)
    return int(parsed.timestamp())


def yahoo_bytes(events):
    rows = {}
    for index, (day, numerator, denominator) in enumerate(events):
        rows[str(index)] = {
            "date": epoch(day),
            "numerator": numerator,
            "denominator": denominator,
            "splitRatio": f"{numerator}:{denominator}",
        }
    return json.dumps({
        "chart": {"result": [{"events": {"splits": rows}}], "error": None},
    }).encode("utf-8")


def sec_bytes(cik, facts):
    rows = []
    for index, (fact_date, ratio) in enumerate(facts):
        rows.append({
            "end": fact_date,
            "val": ratio,
            "accn": f"000000-{index}",
            "form": "10-K",
            "filed": fact_date,
        })
    return json.dumps({
        "cik": cik,
        "facts": {
            "us-gaap": {
                provider_mod.SEC_SPLIT_TAG: {"units": {"pure": rows}},
            },
        },
    }).encode("utf-8")


def ticker_map_bytes(rows):
    return json.dumps({
        str(index): {"ticker": ticker, "cik_str": cik}
        for index, (ticker, cik) in enumerate(rows)
    }).encode("utf-8")


GOOD_ISSUER = (
    b"<html>Fortinet announced that this is the second time the company has "
    b"split its stock. In 2011 it completed a two-for-one stock split.</html>")


class FixtureFetcher:
    def __init__(self, yahoo_by_symbol, sec_by_cik, *, ticker_map=None,
                 issuer=GOOD_ISSUER):
        self.yahoo_by_symbol = dict(yahoo_by_symbol)
        self.sec_by_cik = dict(sec_by_cik)
        self.ticker_map = ticker_map
        self.issuer = issuer
        self.calls = []

    def __call__(self, url, headers=None, timeout=30):
        self.calls.append(url)
        if url == provider_mod.SEC_TICKERS_URL:
            if self.ticker_map is None:
                raise RuntimeError("ticker map unavailable")
            return self.ticker_map
        if url == provider_mod.FTNT_ISSUER_URL:
            if isinstance(self.issuer, Exception):
                raise self.issuer
            return self.issuer
        if "query1.finance.yahoo.com" in url:
            for symbol, payload in self.yahoo_by_symbol.items():
                if f"/chart/{symbol}?" in url:
                    return payload
            raise RuntimeError(f"no Yahoo fixture for {url}")
        if "companyfacts/CIK" in url:
            for cik, payload in self.sec_by_cik.items():
                if f"CIK{int(cik):010d}.json" in url:
                    return payload
            raise RuntimeError(f"no SEC fixture for {url}")
        raise RuntimeError(f"unexpected URL {url}")


base = Path(tempfile.mkdtemp(prefix="split_cache_st_"))
root = base / storage.STORAGE_DIR_NAME
root.mkdir()


# Cache schema, atomic round-trip, scanner namespace, and stale handling.
seed_manifest(root, "NVDA", 4815747)
nvda_identity = cache.manifest_identity(root, "NVDA")
nvda_payload = cache.build_cache_payload(
    nvda_identity, provider_result("NVDA", cik=1045810))
nvda_path, committed = cache.write_cache(root, nvda_identity, nvda_payload)
check("cache: path is under reserved identity namespace",
      nvda_path == root / "_split_history" / "NVDA__4815747.json",
      str(nvda_path))
loaded = cache.load_cache(root, nvda_identity)
check("cache: validated round-trip is usable",
      loaded["status"] == "ok" and loaded["usable"]
      and loaded["cache"]["events"][0]["confirmed"], str(loaded))
check("cache: normalized payload keeps exact manifest identity",
      committed["ticker"] == "NVDA"
      and committed["conid"] == 4815747
      and committed["provider_symbol"] == "NVDA", str(committed))

scan = storage.scan_storage(root, workers=1)
check("cache: storage scanner ignores reserved split-history directory",
      not any("_split_history" in str(row) for row in scan["unrecognized"]),
      str(scan["unrecognized"]))

stale = cache.load_cache(
    root, nvda_identity, max_age=dt.timedelta(days=1),
    now=FIXED_NOW + dt.timedelta(days=2))
check("cache: max-age gate reports stale without deleting payload",
      stale["status"] == "stale" and not stale["usable"]
      and stale.get("cache", {}).get("ticker") == "NVDA", str(stale))

future_payload = dict(nvda_payload)
future_payload["fetched_at"] = (
    FIXED_NOW + dt.timedelta(days=1)).isoformat(timespec="seconds")
future_payload["detection"] = dict(
    future_payload["detection"], reference_asof=future_payload["fetched_at"])
cache.write_cache(root, nvda_identity, future_payload)
future = cache.load_cache(root, nvda_identity, now=FIXED_NOW)
check("cache: future fetched_at is stale even without max-age policy",
      future["status"] == "stale"
      and future.get("reason") == "fetched_at_is_in_the_future", str(future))
cache.write_cache(root, nvda_identity, nvda_payload)

good_nvda_bytes = nvda_path.read_bytes()
storage._atomic_write_bytes(nvda_path, b"{broken")
corrupt = cache.load_cache(root, nvda_identity)
check("cache: malformed JSON is corrupt and unusable",
      corrupt["status"] == "corrupt" and not corrupt["usable"], str(corrupt))
storage._atomic_write_bytes(nvda_path, good_nvda_bytes)

missing_source_payload = dict(nvda_payload)
missing_source_payload["sources"] = {
    "different-source": {"kind": "fixture"},
}
try:
    cache.validate_cache_payload(
        missing_source_payload, expected_identity=nvda_identity)
except cache.CacheValidationError:
    missing_provenance_rejected = True
else:
    missing_provenance_rejected = False
check("cache: every referenced source needs captured provenance",
      missing_provenance_rejected)

candidate_detection = {
    "series_fingerprint": "b" * 64,
    "reference_asof": FIXED_NOW.isoformat(timespec="seconds"),
    "candidates": [{
        "ticker": "NVDA",
        "date": "2024-06-10",
        "factor": 10.0,
        "deep_cv": 0.001,
        "current_anchor_sound": True,
    }],
}
candidate_payload = cache.build_cache_payload(
    nvda_identity, provider_result("NVDA", cik=1045810),
    detection=candidate_detection)
check("cache: candidate rows are normalized into the persisted schema",
      candidate_payload["detection"]["candidates"] == [{
          "ticker": "NVDA",
          "date": "2024-06-10",
          "factor": 10.0,
          "abs_factor": 10.0,
          "deep_cv": 0.001,
          "current_anchor_sound": True,
          "volume": None,
      }], str(candidate_payload["detection"]))

wrong_ticker_detection = dict(candidate_detection)
wrong_ticker_detection["candidates"] = [dict(
    candidate_detection["candidates"][0], ticker="OTHER")]
try:
    cache.build_cache_payload(
        nvda_identity, provider_result("NVDA", cik=1045810),
        detection=wrong_ticker_detection)
except cache.CacheValidationError:
    wrong_candidate_rejected = True
else:
    wrong_candidate_rejected = False
check("cache: candidate ticker must match the manifest identity",
      wrong_candidate_rejected)

malformed_detection = dict(candidate_detection)
malformed_detection["candidates"] = [{
    "ticker": "NVDA", "date": "bad-date", "factor": 10.0,
}]
try:
    cache.build_cache_payload(
        nvda_identity, provider_result("NVDA", cik=1045810),
        detection=malformed_detection)
except cache.CacheValidationError:
    malformed_candidate_rejected = True
else:
    malformed_candidate_rejected = False
check("cache: malformed candidate fails schema validation",
      malformed_candidate_rejected)


# Identity mismatch, rename, provider-symbol mismatch, and unknown conId.
manifest = storage.load_manifest(root / "NVDA")
manifest["conid"] = 999999
storage.save_manifest(root / "NVDA", manifest)
changed_identity = cache.manifest_identity(root, "NVDA")
changed = cache.load_cache(root, changed_identity)
check("cache: changed manifest conId rejects the old cache",
      changed["status"] == "identity_mismatch" and not changed["usable"],
      str(changed))

seed_manifest(root, "OLD", 333, symbol="OLD")
old_identity = cache.manifest_identity(root, "OLD")
cache.write_cache(
    root, old_identity,
    cache.build_cache_payload(old_identity, provider_result("OLD", cik=3333)))
seed_manifest(root, "NEW", 333, symbol="NEW")
renamed = cache.load_cache(root, cache.manifest_identity(root, "NEW"))
check("cache: ticker rename requires a fresh identity-bound cache",
      renamed["status"] == "identity_mismatch" and not renamed["usable"],
      str(renamed))

seed_manifest(root, "SYM", 444, symbol="SYM")
sym_identity = cache.manifest_identity(root, "SYM")
sym_payload = cache.build_cache_payload(
    sym_identity, provider_result("SYM", cik=4444))
sym_payload["provider_symbol"] = "DIFFERENT"
sym_path = cache.cache_path(root, sym_identity)
storage._atomic_write_bytes(
    sym_path, (json.dumps(sym_payload) + "\n").encode("utf-8"))
sym_mismatch = cache.load_cache(root, sym_identity)
check("cache: provider-symbol mismatch is rejected",
      sym_mismatch["status"] == "identity_mismatch", str(sym_mismatch))

seed_manifest(root, "UNKNOWN", None)
unknown_identity = cache.manifest_identity(root, "UNKNOWN")
unknown_payload = cache.build_cache_payload(
    unknown_identity, provider_result("UNKNOWN", cik=5555))
cache.write_cache(root, unknown_identity, unknown_payload)
unknown = cache.load_cache(root, unknown_identity)
check("cache: unknown conId cache is retained but never usable",
      unknown["status"] == "identity_unknown" and not unknown["usable"],
      str(unknown))
never_called = StaticProvider()
unknown_refresh = cache.refresh_ticker(
    root, "UNKNOWN", never_called, **PHASE3_FIXTURES)
check("refresh: missing conId refuses before provider access",
      unknown_refresh["status"] == "error" and not never_called.calls,
      str(unknown_refresh))


# Concurrent writers always leave one complete validated object.
seed_manifest(root, "CONC", 777)
conc_identity = cache.manifest_identity(root, "CONC")
write_errors = []


def concurrent_writer(index):
    try:
        payload = cache.build_cache_payload(
            conc_identity,
            provider_result("CONC", cik=7777, marker=index))
        payload["writer"] = index
        cache.write_cache(root, conc_identity, payload)
    except Exception as exc:  # noqa: BLE001 - test captures every thread
        write_errors.append(str(exc))


threads = [threading.Thread(target=concurrent_writer, args=(index,))
           for index in range(20)]
for thread in threads:
    thread.start()
for thread in threads:
    thread.join()
conc = cache.load_cache(root, conc_identity)
check("cache: concurrent atomic writes leave a valid complete cache",
      not write_errors and conc["status"] == "ok"
      and conc["cache"].get("writer") in range(20),
      str((write_errors, conc)))
check("cache: concurrent writes leave no temporary files",
      not list(cache.cache_root(root).glob("*.tmp")))


# Refresh success and all failure paths preserve manifest/last-good bytes.
seed_manifest(root, "REFRESH", 888)
refresh_manifest = (root / "REFRESH" / storage.MANIFEST_NAME).read_bytes()
static = StaticProvider()
refresh = cache.refresh_ticker(
    root, "REFRESH", static, cik=8888, **PHASE3_FIXTURES)
check("refresh: explicit provider result writes one valid cache",
      refresh["status"] == "refreshed"
      and len(static.calls) == 1
      and cache.load_cache(
          root, cache.manifest_identity(root, "REFRESH"))["status"] == "ok",
      str(refresh))
check("refresh: cache write never changes ticker manifest",
      (root / "REFRESH" / storage.MANIFEST_NAME).read_bytes()
      == refresh_manifest)

refresh_path = Path(refresh["path"])
last_good = refresh_path.read_bytes()
failed = cache.refresh_ticker(
    root, "REFRESH", StaticProvider(error=RuntimeError("source down")),
    **PHASE3_FIXTURES)
check("refresh: source failure preserves last-good bytes",
      failed["status"] == "error" and failed["preserved_last_good"]
      and refresh_path.read_bytes() == last_good, str(failed))

malformed = StaticProvider(result_fn=lambda *_args: {"provider_status": "ok"})
malformed_reference_calls = []
bad_refresh = cache.refresh_ticker(
    root, "REFRESH", malformed,
    reference_fetcher=lambda *_args, **_kwargs: malformed_reference_calls.append(1),
    scan_fn=clean_scan)
check("refresh: malformed provider result preserves last-good bytes",
      bad_refresh["status"] == "error"
      and bad_refresh["preserved_last_good"]
      and refresh_path.read_bytes() == last_good
      and not malformed_reference_calls, str(bad_refresh))


# Phase 3A refresh couples provider history to independent daily candidates.
seed_manifest(root, "CAND", 889)
reference_calls = []
scan_calls = []


def candidate_reference(symbol, rng="Max"):
    reference_calls.append((symbol, rng))
    return FIXTURE_REFERENCE


def candidate_scan(scan_root, ticker, ref=None):
    scan_calls.append((Path(scan_root), ticker, ref))
    return {
        "ticker": ticker,
        "verdict": "SEAM",
        "series_fingerprint": "c" * 64,
        "candidates": [{
            "ticker": ticker,
            "date": "2014-01-10",
            "boundary": "2014-01-10",
            "factor": 4.0,
            "deep_cv": 0.0009,
            "current_anchor_sound": True,
            "strength": 1.38,
        }],
    }


candidate_refresh = cache.refresh_ticker(
    root, "CAND", StaticProvider(), cik=8899,
    reference_fetcher=candidate_reference, scan_fn=candidate_scan)
candidate_identity = cache.manifest_identity(root, "CAND")
candidate_loaded = cache.load_cache(root, candidate_identity)
check("refresh: explicit refresh stores fingerprinted candidates",
      candidate_refresh["status"] == "refreshed"
      and candidate_refresh["candidate_count"] == 1
      and candidate_refresh["series_fingerprint"] == "c" * 64
      and candidate_loaded["cache"]["detection"]["candidates"][0]["factor"]
      == 4.0, str((candidate_refresh, candidate_loaded)))
check("refresh: daily reference and scanner receive the resolved identity",
      reference_calls == [("CAND", "Max")]
      and scan_calls == [(root, "CAND", FIXTURE_REFERENCE)],
      str((reference_calls, scan_calls)))

candidate_path = Path(candidate_refresh["path"])
candidate_good = candidate_path.read_bytes()
reference_failed = cache.refresh_ticker(
    root, "CAND", StaticProvider(), cik=8899,
    reference_fetcher=lambda *_args, **_kwargs: (
        (_ for _ in ()).throw(RuntimeError("daily reference down"))),
    scan_fn=candidate_scan)
check("refresh: daily-reference failure preserves last-good bytes",
      reference_failed["status"] == "error"
      and reference_failed["preserved_last_good"]
      and candidate_path.read_bytes() == candidate_good,
      str(reference_failed))

malformed_reference_scan_calls = []
malformed_reference = cache.refresh_ticker(
    root, "CAND", StaticProvider(), cik=8899,
    reference_fetcher=lambda *_args, **_kwargs: None,
    scan_fn=lambda *_args, **_kwargs: malformed_reference_scan_calls.append(1))
check("refresh: malformed daily reference fails before scanner access",
      malformed_reference["status"] == "error"
      and malformed_reference["preserved_last_good"]
      and not malformed_reference_scan_calls
      and candidate_path.read_bytes() == candidate_good,
      str(malformed_reference))

scan_failed = cache.refresh_ticker(
    root, "CAND", StaticProvider(), cik=8899,
    reference_fetcher=fixture_reference,
    scan_fn=lambda _root, ticker, ref=None: {
        "ticker": ticker,
        "verdict": "UNVERIFIABLE",
        "why": "manifest race",
        "candidates": [],
    })
check("refresh: unusable candidate scan preserves last-good bytes",
      scan_failed["status"] == "error"
      and scan_failed["preserved_last_good"]
      and candidate_path.read_bytes() == candidate_good,
      str(scan_failed))

short_scan = cache.refresh_ticker(
    root, "CAND", StaticProvider(), cik=8899,
    reference_fetcher=fixture_reference,
    scan_fn=lambda _root, ticker, ref=None: {
        "ticker": ticker,
        "verdict": "SHORT",
        "n": 25,
        "series_fingerprint": "c" * 64,
        "candidates": [],
    })
check("refresh: SHORT detection preserves last-good bytes",
      short_scan["status"] == "error"
      and short_scan["preserved_last_good"]
      and candidate_path.read_bytes() == candidate_good,
      str(short_scan))

mismatched_candidate = cache.refresh_ticker(
    root, "CAND", StaticProvider(), cik=8899,
    reference_fetcher=fixture_reference,
    scan_fn=lambda _root, ticker, ref=None: {
        "ticker": ticker,
        "verdict": "SEAM",
        "series_fingerprint": "d" * 64,
        "candidates": [{
            "ticker": "OTHER",
            "date": "2014-01-10",
            "factor": 4.0,
            "deep_cv": 0.001,
            "current_anchor_sound": True,
        }],
    })
check("refresh: identity-unsafe candidate preserves last-good bytes",
      mismatched_candidate["status"] == "error"
      and mismatched_candidate["preserved_last_good"]
      and candidate_path.read_bytes() == candidate_good,
      str(mismatched_candidate))

bad_fingerprint = cache.refresh_ticker(
    root, "CAND", StaticProvider(), cik=8899,
    reference_fetcher=fixture_reference,
    scan_fn=lambda _root, ticker, ref=None: {
        "ticker": ticker,
        "verdict": "CLEAN",
        "series_fingerprint": "not-a-digest",
        "candidates": [],
    })
check("refresh: invalid detection fingerprint preserves last-good bytes",
      bad_fingerprint["status"] == "error"
      and bad_fingerprint["preserved_last_good"]
      and candidate_path.read_bytes() == candidate_good,
      str(bad_fingerprint))


# Temporary-storage end-to-end: real month reads and default candidate scanner.
e2e_reference = seed_daily_ratio_series(
    root, "E2E", 890, [0.25] * 120 + [1.0] * 120)
e2e_manifest_path = root / "E2E" / storage.MANIFEST_NAME
e2e_manifest_before = e2e_manifest_path.read_bytes()
e2e = cache.refresh_ticker(
    root, "E2E", StaticProvider(), cik=8900,
    reference_fetcher=lambda _symbol, rng="Max": e2e_reference)
e2e_loaded = cache.load_cache(root, cache.manifest_identity(root, "E2E"))
e2e_candidates = e2e_loaded.get("cache", {}).get("detection", {}).get(
    "candidates", [])
check("integration: real temp daily series flows through default scanner",
      e2e["status"] == "refreshed" and e2e["candidate_count"] == 1
      and e2e_candidates[0]["factor"] == 4.0
      and e2e_candidates[0]["current_anchor_sound"] is True,
      str((e2e, e2e_loaded)))
check("integration: refresh leaves the source manifest byte-identical",
      e2e_manifest_path.read_bytes() == e2e_manifest_before)


# Proven production provider fixtures: history range, reverse split, CIK, and
# captured FTNT complete evidence.
class FixtureYahooSecProvider(provider_mod.YahooSecProvider):
    """Run retained A2 parser/retry fixtures, never the default transport."""
    def _fetch(self, url, headers=None):
        assert isinstance(self.fetcher, FixtureFetcher)
        with patch("fetch_ibkr_bridge.refuse_a2"):
            return super()._fetch(url, headers)


from fetch_run_context import RequestRefused
hold_calls = []
held_provider = provider_mod.YahooSecProvider(fetcher=lambda *a, **k: hold_calls.append(a))
try:
    held_provider._fetch("https://example.com")
except RequestRefused:
    check("A1 holds split provider before even an injected fetcher", not hold_calls)
else:
    check("A1 holds split provider", False)

ftnt_fetcher = FixtureFetcher(
    {"FTNT": yahoo_bytes([
        ("2011-06-02", 2, 1),
        ("2022-06-23", 5, 1),
    ])},
    {1262039: sec_bytes(1262039, [("2022-06-23", 5.0)])})
ftnt_provider = FixtureYahooSecProvider(
    fetcher=ftnt_fetcher, attempts=1, clock=lambda: FIXED_NOW,
    cik_overrides={"FTNT": 1262039})
ftnt_result = ftnt_provider.fetch_history("FTNT", "FTNT")
check("provider: FTNT events are issuer/SEC confirmed",
      [event["ex_date"] for event in ftnt_result["events"]]
      == ["2011-06-02", "2022-06-23"]
      and all(event["confidence"] == "confirmed"
              for event in ftnt_result["events"]), str(ftnt_result["events"]))
check("provider: captured FTNT evidence derives complete coverage",
      ftnt_result["coverage"]["complete"]
      and ftnt_result["coverage"]["evidence_level"]
      == "issuer_explicit_history"
      and ftnt_result["coverage"]["complete_basis"]["payload_sha256"],
      str(ftnt_result["coverage"]))

seed_manifest(root, "FTNT", 3691937)
ftnt_refresh = cache.refresh_ticker(
    root, "FTNT", ftnt_provider, **PHASE3_FIXTURES)
check("refresh: production FTNT fixture writes complete cache",
      ftnt_refresh["status"] == "refreshed"
      and ftnt_refresh["coverage_complete"], str(ftnt_refresh))
ftnt_cache_path = Path(ftnt_refresh["path"])
ftnt_good = ftnt_cache_path.read_bytes()
ftnt_fetcher.issuer = b"<html>issuer page changed</html>"
changed_issuer = cache.refresh_ticker(
    root, "FTNT", ftnt_provider, **PHASE3_FIXTURES)
check("refresh: changed issuer page preserves captured last-good evidence",
      changed_issuer["status"] == "error"
      and changed_issuer["preserved_last_good"]
      and ftnt_cache_path.read_bytes() == ftnt_good,
      str(changed_issuer))

missing_event_fetcher = FixtureFetcher(
    {"FTNT": yahoo_bytes([("2022-06-23", 5, 1)])},
    {1262039: sec_bytes(1262039, [("2022-06-23", 5.0)])})
missing_event_provider = FixtureYahooSecProvider(
    fetcher=missing_event_fetcher, attempts=1, clock=lambda: FIXED_NOW,
    cik_overrides={"FTNT": 1262039})
try:
    missing_event_provider.fetch_history("FTNT", "FTNT")
except provider_mod.ProviderError:
    incomplete_issuer_history_rejected = True
else:
    incomplete_issuer_history_rejected = False
check("provider: complete issuer claim requires both enumerated FTNT events",
      incomplete_issuer_history_rejected)

aapl_fetcher = FixtureFetcher(
    {"AAPL": yahoo_bytes([
        ("2005-02-28", 2, 1),
        ("2014-06-09", 7, 1),
    ])},
    {320193: sec_bytes(320193, [("2014-06-09", 7.0)])})
aapl_provider = FixtureYahooSecProvider(
    fetcher=aapl_fetcher, attempts=1, clock=lambda: FIXED_NOW,
    cik_overrides={"AAPL": 320193})
aapl = aapl_provider.fetch_history("AAPL", "AAPL")
historical = ["2011-06-02", "2005-02-28", "2014-06-09"]
check("provider: three pre-2015 events across two issuers parse",
      set(historical) == {
          ftnt_result["events"][0]["ex_date"],
          aapl["events"][0]["ex_date"],
          aapl["events"][1]["ex_date"],
      }, str((ftnt_result["events"], aapl["events"])))
check("provider: pre-XBRL event without SEC fact remains provisional",
      aapl["events"][0]["ex_date"] == "2005-02-28"
      and aapl["events"][0]["confidence"] == "provisional"
      and not aapl["coverage"]["complete"], str(aapl))

ge_fetcher = FixtureFetcher(
    {"GE": yahoo_bytes([("2021-08-02", 1, 8)])},
    {40545: sec_bytes(40545, [("2021-08-02", 0.125)])})
ge_provider = FixtureYahooSecProvider(
    fetcher=ge_fetcher, attempts=1, clock=lambda: FIXED_NOW,
    cik_overrides={"GE": 40545})
ge = ge_provider.fetch_history("GE", "GE")
check("provider: confirmed reverse ratio keeps new/old convention",
      ge["events"][0]["ratio"] == 0.125
      and ge["events"][0]["confidence"] == "confirmed", str(ge["events"]))

map_fetcher = FixtureFetcher(
    {"MSFT": yahoo_bytes([])},
    {789019: sec_bytes(789019, [])},
    ticker_map=ticker_map_bytes([("MSFT", 789019)]))
map_provider = FixtureYahooSecProvider(
    fetcher=map_fetcher, attempts=1, clock=lambda: FIXED_NOW)
mapped = map_provider.fetch_history("MSFT", "MSFT")
check("provider: SEC ticker map explicitly resolves missing CIK binding",
      mapped["cik"] == 789019
      and provider_mod.SEC_TICKERS_URL in map_fetcher.calls, str(mapped))

bad_cik_fetcher = FixtureFetcher(
    {"BAD": yahoo_bytes([])},
    {123: sec_bytes(999, [])})
bad_cik_provider = FixtureYahooSecProvider(
    fetcher=bad_cik_fetcher, attempts=1, clock=lambda: FIXED_NOW,
    cik_overrides={"BAD": 123})
try:
    bad_cik_provider.fetch_history("BAD", "BAD")
except provider_mod.ProviderError:
    bad_cik_rejected = True
else:
    bad_cik_rejected = False
check("provider: CompanyFacts CIK mismatch fails closed", bad_cik_rejected)


# Bank pacing and CLI default-offline behavior.
for ticker, conid in (("PACEA", 901), ("PACEB", 902)):
    seed_manifest(root, ticker, conid)
pace_provider = StaticProvider()
sleeps = []
paced = cache.refresh_many(
    root, ["PACEB", "PACEA"], pace_provider,
    cik_bindings={"PACEA": 1, "PACEB": 2},
    pace_seconds=0.5, limit=2, sleep_fn=sleeps.append,
    **PHASE3_FIXTURES)
check("refresh-bank: sequential work is paced and bounded",
      paced["processed"] == 2 and paced["failures"] == 0
      and sleeps == [0.5], str((paced, sleeps)))

offline_root = base / "offline-only" / storage.STORAGE_DIR_NAME
offline_root.mkdir(parents=True)
seed_manifest(offline_root, "OFF", 123)
bomb = StaticProvider(error=AssertionError("offline mode called provider"))
stdout = io.StringIO()
with contextlib.redirect_stdout(stdout):
    offline_code = cache.main(
        ["--root", str(offline_root)], provider=bomb,
        reference_fetcher=lambda *_args, **_kwargs: (
            (_ for _ in ()).throw(AssertionError("offline reference call"))),
        scan_fn=lambda *_args, **_kwargs: (
            (_ for _ in ()).throw(AssertionError("offline scanner call"))))
check("cli: default mode is offline and performs no provider call",
      offline_code == 0 and not bomb.calls
      and not cache.cache_root(offline_root).exists(), stdout.getvalue())

cli_provider = StaticProvider()
artifact = base / "refresh-artifact.json"
with contextlib.redirect_stdout(io.StringIO()):
    cli_code = cache.main([
        "--root", str(offline_root),
        "--refresh", "OFF",
        "--cik", "OFF=1234",
        "--pace", "0",
        "--artifact", str(artifact),
    ], provider=cli_provider, **PHASE3_FIXTURES)
check("cli: explicit refresh writes cache and requested artifact only",
      cli_code == 0 and len(cli_provider.calls) == 1
      and artifact.is_file()
      and cache.cache_path(
          offline_root, cache.manifest_identity(offline_root, "OFF")).is_file(),
      artifact.read_text(encoding="utf-8") if artifact.exists() else "missing")

# GUI-facing progress/cancel contract: cancellation is checked before network
# and after every potentially blocking phase, before the atomic cache commit.
pre_cancel = threading.Event()
pre_cancel.set()
pre_cancel_provider = StaticProvider()
pre_cancelled = cache.refresh_ticker(
    offline_root, "OFF", pre_cancel_provider,
    cancel=pre_cancel.is_set, **PHASE3_FIXTURES)
check("refresh cancel: pre-cancel refuses before provider or cache write",
      pre_cancelled["status"] == "cancelled"
      and pre_cancelled["stage"] == "before_identity"
      and not pre_cancel_provider.calls,
      str(pre_cancelled))

cancel_root = base / "cancel" / storage.STORAGE_DIR_NAME
cancel_root.mkdir(parents=True)
seed_manifest(cancel_root, "CXL", 8181)
cancel_after_provider = threading.Event()


class CancellingProvider(StaticProvider):
    def fetch_history(self, ticker, provider_symbol=None, cik=None):
        result = super().fetch_history(ticker, provider_symbol, cik)
        cancel_after_provider.set()
        return result


cancel_refs = []
cancel_provider = CancellingProvider()
cancelled = cache.refresh_ticker(
    cancel_root, "CXL", cancel_provider,
    reference_fetcher=lambda *_args, **_kwargs: cancel_refs.append(1),
    scan_fn=clean_scan, cancel=cancel_after_provider.is_set)
cancel_identity = cache.manifest_identity(cancel_root, "CXL")
check("refresh cancel: post-provider cancel stops before reference and commit",
      cancelled["status"] == "cancelled"
      and cancelled["stage"] == "after_provider_history"
      and len(cancel_provider.calls) == 1 and not cancel_refs
      and not cache.cache_path(cancel_root, cancel_identity).exists(),
      str(cancelled))

progress = []
progress_provider = StaticProvider()
progress_result = cache.refresh_ticker(
    root, "REFRESH", progress_provider,
    progress=lambda stage, ticker: progress.append((stage, ticker)),
    **PHASE3_FIXTURES)
check("refresh progress: successful selected-ticker stages are ordered",
      progress_result["status"] == "refreshed"
      and [stage for stage, _ticker in progress] == [
          "identity", "provider_history", "daily_reference",
          "candidate_scan", "commit", "done"],
      str((progress_result, progress)))


print(f"\n{N[0]} checks, {len(FAILS)} failed")
if FAILS:
    print("FAILED:", ", ".join(FAILS))
    raise SystemExit(1)
print("ALL PASS")
