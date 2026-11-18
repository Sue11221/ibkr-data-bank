"""Pure offline cache/schema/default-hold/CLI tests.

The 22 refresh-root and eight provider-composition assertions moved to the
registered guarded HTTP suite under plan46.6/47.2; none is parser-only credit.
Filesystem writes remain confined to a synthetic temporary storage root.
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


from fetch_run_context import RequestRefused
hold_calls = []
held_provider = provider_mod.YahooSecProvider(fetcher=lambda *a, **k: hold_calls.append(a))
try:
    held_provider._fetch("https://example.com")
except RequestRefused:
    check("A1 holds split provider before even an injected fetcher", not hold_calls)
else:
    check("A1 holds split provider", False)

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

print(f"\n{N[0]} checks, {len(FAILS)} failed")
if FAILS:
    print("FAILED:", ", ".join(FAILS))
    raise SystemExit(1)
print("ALL PASS")
