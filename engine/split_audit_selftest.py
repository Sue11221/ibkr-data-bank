"""Offline self-tests for the Phase 3B split audit."""

from __future__ import annotations

import contextlib
import datetime as dt
import io
import json
import socket
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import deep_seam_scan as seam  # noqa: E402
import split_audit as audit  # noqa: E402
import split_cache as cache  # noqa: E402
import stock_storage as storage  # noqa: E402


FAILS = []
N = [0]
FIXED_NOW = dt.datetime(2026, 7, 10, 12, 0, tzinfo=dt.timezone.utc)


def check(name, condition, detail=""):
    N[0] += 1
    print(f"[{'PASS' if condition else 'FAIL'}] {name}"
          + (f" -- {detail}" if detail and not condition else ""))
    if not condition:
        FAILS.append(name)


def bar(day, close, open_=None):
    stamp = dt.datetime.combine(dt.date.fromisoformat(day), dt.time())
    open_ = close if open_ is None else open_
    high, low = max(open_, close), min(open_, close)
    return stamp, open_, high, low, close, 1000


def seed_ticker(root, ticker, conid, bars, correction=None):
    ticker_dir = Path(root) / ticker
    ticker_dir.mkdir(parents=True, exist_ok=True)
    manifest = storage.new_manifest(ticker, ticker)
    manifest["conid"] = conid
    if correction is not None:
        manifest["data_corrections"] = correction
    grouped = {}
    for item in bars:
        grouped.setdefault(item[0].date().isoformat()[:7], []).append(item)
    months = storage.manifest_months(manifest, "1d")
    for month, month_bars in sorted(grouped.items()):
        year, number = map(int, month.split("-"))
        path = storage.month_file_path(
            root, ticker, year, number, "1d")
        months[month] = storage.write_month_file(path, month_bars)
    storage.save_manifest(ticker_dir, manifest)
    manifest = storage.load_manifest(ticker_dir)
    return seam.manifest_fingerprint(storage.manifest_months(manifest, "1d"))


def complete_coverage(source_id="fixture-source"):
    return {
        "from": "2000-01-01",
        "through": "2026-07-10",
        "evidence_level": "issuer_explicit_history",
        "complete": True,
        "complete_basis": {
            "kind": "issuer_explicit_history",
            "payload_sha256": "e" * 64,
            "captured_at": FIXED_NOW.isoformat(timespec="seconds"),
            "reason": "fixture issuer explicitly enumerates complete history",
            "source_url": "https://issuer.invalid/splits",
            "statement_excerpt": "Complete split history fixture.",
            "source_ids": [source_id],
        },
        "source_ids": [source_id],
    }


def provisional_coverage(source_id="fixture-source"):
    return {
        "from": "2000-01-01",
        "through": "2026-07-10",
        "evidence_level": "provisional",
        "complete": False,
        "complete_basis": None,
        "source_ids": [source_id],
    }


def event(day, ratio, source_id="fixture-source"):
    return {
        "ex_date": day,
        "ratio": ratio,
        "confidence": "confirmed",
        "source_ids": [source_id],
    }


def provider_result(ticker, events=None, coverage=None):
    source_id = "fixture-source"
    return {
        "ticker": ticker,
        "provider_symbol": ticker,
        "cik": 100000 + len(ticker),
        "fetched_at": FIXED_NOW.isoformat(timespec="seconds"),
        "provider": "offline-fixture-v1",
        "provider_status": "ok",
        "coverage": coverage or complete_coverage(source_id),
        "events": list(events or []),
        "sources": {
            source_id: {
                "kind": "captured-fixture",
                "payload_sha256": "e" * 64,
            },
        },
    }


def candidate(ticker, day, factor, deep_cv=0.001,
              current_anchor_sound=True):
    return {
        "ticker": ticker,
        "date": day,
        "factor": factor,
        "deep_cv": deep_cv,
        "current_anchor_sound": current_anchor_sound,
        "volume": None,
    }


def write_cache(root, ticker, fingerprint, *, events=None, coverage=None,
                candidates=None, fetched_at=None):
    identity = cache.manifest_identity(root, ticker)
    result = provider_result(ticker, events=events, coverage=coverage)
    if fetched_at is not None:
        result["fetched_at"] = fetched_at
    payload = cache.build_cache_payload(
        identity, result,
        detection={
            "series_fingerprint": fingerprint,
            "reference_asof": FIXED_NOW.isoformat(timespec="seconds"),
            "candidates": list(candidates or []),
        })
    return cache.write_cache(root, identity, payload)[0]


def correction(ticker, kind, day, *, factor=None, deep_cv=None):
    row = {
        "ticker": ticker,
        "type": kind,
        "applied": "2026-07-09T12:00:00-04:00",
    }
    if kind in storage.IDENTITY_CORRECTION_TYPES:
        row["cutover"] = day
        row["intervals"] = ["1d"]
    else:
        row["ex_date"] = day
    if factor is not None:
        row["factor"] = factor
    if deep_cv is not None:
        row["triage_evidence"] = {
            "deep_cv": deep_cv,
            "r_now": 1.0,
        }
    return row


def snapshot(root, exclude=()):
    excluded = set(exclude)
    return {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in sorted(Path(root).rglob("*"))
        if path.is_file() and path.name not in excluded
    }


def archive_payload():
    return {
        "run": "deep-seam-scan-20260709-codex",
        "read_only": True,
        "rows": [
            {
                "ticker": "FTNT", "verdict": "SEAM",
                "boundary": "2014-01-10", "factor": 4.0073,
                "abs_factor": 4.007, "split_like": True,
            },
            {
                "ticker": "WBD", "verdict": "SEAM",
                "boundary": "2021-03-30", "factor": 2.029,
                "abs_factor": 2.029, "split_like": True,
            },
            {"ticker": "NVDA", "verdict": "CLEAN", "max_step": 1.01},
        ],
        "triage": {
            "confirmed_phantom_split_tickers": [{
                "ticker": "FTNT", "boundary": "2014-01-10",
                "ex_date": "2014-01-13",
                "classification": "PHANTOM_SPLIT_CONFIRMED",
            }],
            "split_like_review_tickers": [{
                "ticker": "WBD", "boundary": "2021-03-30",
                "classification": "SPLIT_LIKE_REVIEW_NOT_CONFIRMED_PHANTOM",
            }],
        },
    }


base = Path(tempfile.mkdtemp(prefix="split_audit_st_"))
root = base / "bank"
root.mkdir()

ftnt_correction = correction(
    "FTNT", "phantom_split_correction", "2014-01-13",
    factor=4.0, deep_cv=0.0009)
wbd_correction = correction(
    "WBD", "identity_truncation", "2021-03-30", deep_cv=0.1289)

common_bars = [bar("2026-07-08", 100), bar("2026-07-09", 101)]
ftnt_fp = seed_ticker(root, "FTNT", 101, common_bars, ftnt_correction)
wbd_fp = seed_ticker(root, "WBD", 102, common_bars, wbd_correction)
nvda_fp = seed_ticker(root, "NVDA", 103, [
    bar("2024-06-07", 100),
    bar("2024-06-10", 101, open_=100),
    bar("2024-06-11", 102),
])
write_cache(root, "FTNT", ftnt_fp)
write_cache(root, "WBD", wbd_fp)
write_cache(root, "NVDA", nvda_fp,
            events=[event("2024-06-10", 10.0)])

archive_path = base / "deep-seam-scan-20260709-codex.json"
archive_path.write_text(
    json.dumps(archive_payload(), sort_keys=True), encoding="utf-8")

loaded_archive = audit.load_archive(archive_path)
check("archive: approved deep-seam shape yields two candidates",
      loaded_archive["candidate_count"] == 2
      and not loaded_archive["errors"], str(loaded_archive))
check("archive: FTNT triage ex-date supersedes rolling boundary",
      loaded_archive["candidates"][0]["candidate"]["date"]
      == "2014-01-13", str(loaded_archive["candidates"]))

before_read = snapshot(root)
original_socket = socket.socket
socket.socket = lambda *args, **kwargs: (_ for _ in ()).throw(
    AssertionError("network attempted"))
try:
    accepted = audit.audit(
        root, ["FTNT", "WBD", "NVDA"], archive_path=archive_path,
        now=FIXED_NOW)
finally:
    socket.socket = original_socket
check("offline: audit completes while socket creation is forbidden",
      accepted["network"] is False)
check("report-only: default audit leaves every bank byte unchanged",
      snapshot(root) == before_read)

by_ticker = {row["ticker"]: row for row in accepted["tickers"]}
ftnt_current = by_ticker["FTNT"]["rows"]
ftnt_archive = by_ticker["FTNT"]["archived_rows"]
wbd_current = by_ticker["WBD"]["rows"]
wbd_archive = by_ticker["WBD"]["archived_rows"]
nvda_current = by_ticker["NVDA"]["rows"]
check("acceptance: archived FTNT classifies PHANTOM",
      [row["verdict"] for row in ftnt_archive] == ["PHANTOM"],
      str(ftnt_archive))
check("acceptance: archived evidence enrichment is explicit",
      ftnt_archive[0]["evidence"]["provenance"][
          "captured_correction_evidence"]
      == ["deep_cv", "r_now_to_current_anchor_sound"], str(ftnt_archive))
check("acceptance: archived PHANTOM cannot enter current queue",
      ftnt_archive[0]["repair_queue"] is False
      and ftnt_archive[0]["current_bank_effect"] is False)
check("acceptance: corrected current FTNT is RESOLVED",
      "RESOLVED" in [row["verdict"] for row in ftnt_current]
      and "REGRESSION" not in [row["verdict"] for row in ftnt_current],
      str(ftnt_current))
check("acceptance: historical WBD remains IDENTITY_BASIS evidence",
      [row["verdict"] for row in wbd_archive] == ["IDENTITY_BASIS"],
      str(wbd_archive))
check("acceptance: current WBD identity truncation is RESOLVED",
      "RESOLVED" in [row["verdict"] for row in wbd_current],
      str(wbd_current))
check("acceptance: NVDA confirmed 2024 split is audited REAL",
      any(row["row_type"] == "event" and row["verdict"] == "REAL"
          for row in nvda_current), str(nvda_current))
check("acceptance: refreshed current fixture has no severe split verdict",
      all(accepted["verdict_counts"][name] == 0
          for name in ("PHANTOM", "MISSING", "REGRESSION")),
      str(accepted["verdict_counts"]))
check("acceptance: refreshed current fixture is complete and queue-clean",
      accepted["summary"]["data_clean"]
      and accepted["summary"]["evidence_complete"]
      and accepted["summary"]["repair_queue_count"] == 0,
      str(accepted["summary"]))
check("report: every policy verdict has a stable count key",
      set(accepted["verdict_counts"]) == set(audit.VERDICTS))
check("report: source provenance is retained per ticker",
      by_ticker["NVDA"]["source_provenance"]["provider"]
      == "offline-fixture-v1")

# Missing adjustment: raw post-split prices retain the inverse 1/4 jump.
raw_fp = seed_ticker(root, "RAW", 104, [
    bar("2020-01-02", 100),
    bar("2020-01-03", 25, open_=25),
    bar("2020-01-06", 26),
])
write_cache(root, "RAW", raw_fp, events=[event("2020-01-03", 4.0)])
raw_report = audit.audit(root, ["RAW"], now=FIXED_NOW)
raw_rows = raw_report["tickers"][0]["rows"]
check("events: confirmed raw split jump is MISSING",
      any(row["row_type"] == "event" and row["verdict"] == "MISSING"
          for row in raw_rows), str(raw_rows))
check("events: MISSING enters the current repair queue",
      raw_report["summary"]["repair_queue_count"] == 1
      and not raw_report["summary"]["data_clean"], str(raw_report["summary"]))

# Every confirmed event receives its own current adjustment row.
multi_fp = seed_ticker(root, "MULTI", 105, [
    bar("2020-01-02", 100), bar("2020-01-03", 101),
    bar("2020-01-06", 102),
    bar("2021-01-04", 100), bar("2021-01-05", 50),
    bar("2021-01-06", 51),
])
write_cache(root, "MULTI", multi_fp, events=[
    event("2020-01-03", 2.0), event("2021-01-05", 2.0),
])
multi_report = audit.audit(root, ["MULTI"], now=FIXED_NOW)
multi_events = [row for row in multi_report["tickers"][0]["rows"]
                if row["row_type"] == "event"]
check("events: every confirmed event is audited",
      len(multi_events) == 2
      and {row["verdict"] for row in multi_events} == {"REAL", "MISSING"},
      str(multi_events))

# A current candidate can be severe only with a matching current fingerprint.
phantom_fp = seed_ticker(root, "PHAN", 106, common_bars)
write_cache(root, "PHAN", phantom_fp,
            candidates=[candidate("PHAN", "2020-01-03", 4.0)])
phantom_report = audit.audit(root, ["PHAN"], now=FIXED_NOW)
check("candidate: current complete-history seam is PHANTOM",
      phantom_report["verdict_counts"]["PHANTOM"] == 1,
      str(phantom_report["tickers"][0]["rows"]))

stale_fp = seed_ticker(root, "STALE", 107, common_bars)
write_cache(root, "STALE", "a" * 64,
            candidates=[candidate("STALE", "2020-01-03", 4.0)])
stale_report = audit.audit(root, ["STALE"], now=FIXED_NOW)
stale_rows = stale_report["tickers"][0]["rows"]
check("fingerprint: stale candidate is not reclassified PHANTOM",
      any(row["verdict"] == "STALE" for row in stale_rows)
      and not any(row["verdict"] == "PHANTOM" for row in stale_rows),
      str(stale_rows))

age_fp = seed_ticker(root, "AGE", 114, common_bars)
write_cache(
    root, "AGE", age_fp,
    fetched_at="2026-06-01T12:00:00+00:00")
age_report = audit.audit(
    root, ["AGE"], now=FIXED_NOW, max_age=dt.timedelta(days=1))
age_ticker = age_report["tickers"][0]
check("cache: age-stale payload retains its validated fingerprint status",
      age_ticker["cache"]["status"] == "stale"
      and age_ticker["fingerprint"]["status"] == "cache_stale"
      and age_ticker["fingerprint"]["cached"] == age_fp,
      str(age_ticker["fingerprint"]))
check("cache: age-stale payload retains source provenance for diagnosis",
      age_ticker["source_provenance"]["provider"] == "offline-fixture-v1")

inc_fp = seed_ticker(root, "INC", 108, common_bars)
write_cache(root, "INC", inc_fp,
            coverage=provisional_coverage(),
            candidates=[candidate("INC", "2020-01-03", 4.0)])
inc_report = audit.audit(root, ["INC"], now=FIXED_NOW)
check("candidate: incomplete negative history is needs_confirmation only",
      inc_report["summary"]["needs_confirmation_count"] == 1
      and inc_report["summary"]["repair_queue_count"] == 0,
      str(inc_report["summary"]))

# A corrected candidate recurrence is emitted once as REGRESSION.
reg_correction = correction(
    "REG", "phantom_split_correction", "2020-01-03", factor=4.0,
    deep_cv=0.001)
reg_fp = seed_ticker(root, "REG", 109, common_bars, reg_correction)
write_cache(root, "REG", reg_fp,
            candidates=[candidate("REG", "2020-01-03", 4.0)])
reg_report = audit.audit(root, ["REG"], now=FIXED_NOW)
check("correction: recurrence is one REGRESSION repair row",
      reg_report["verdict_counts"]["REGRESSION"] == 1
      and reg_report["summary"]["repair_queue_count"] == 1,
      str(reg_report["tickers"][0]["rows"]))

# Missing cache is an operational warning, not candidate confirmation work.
missing_fp = seed_ticker(root, "NOCACHE", 110, common_bars)
assert missing_fp
missing_report = audit.audit(root, ["NOCACHE"], now=FIXED_NOW)
check("cache: missing source is operational but not needs_confirmation",
      missing_report["summary"]["operational_warning_count"] == 1
      and missing_report["summary"]["needs_confirmation_count"] == 0
      and missing_report["summary"]["data_clean"],
      str(missing_report["summary"]))

# Corrupt cache bytes fail closed and remain byte-identical.
corrupt_fp = seed_ticker(root, "CORRUPT", 111, common_bars)
assert corrupt_fp
corrupt_identity = cache.manifest_identity(root, "CORRUPT")
corrupt_path = cache.cache_path(root, corrupt_identity)
corrupt_path.parent.mkdir(parents=True, exist_ok=True)
corrupt_path.write_bytes(b"{not-json")
corrupt_before = corrupt_path.read_bytes()
corrupt_report = audit.audit(root, ["CORRUPT"], now=FIXED_NOW)
check("cache: corrupt payload is UNVERIFIABLE without rewrite",
      corrupt_report["verdict_counts"]["UNVERIFIABLE"] == 1
      and corrupt_path.read_bytes() == corrupt_before,
      str(corrupt_report["tickers"][0]["rows"]))

# Strict event reads reject both stale manifest metadata and read-time races.
diverge_fp = seed_ticker(root, "DIVERGE", 112, [
    bar("2020-01-02", 100), bar("2020-01-03", 101),
])
write_cache(root, "DIVERGE", diverge_fp,
            events=[event("2020-01-03", 2.0)])
diverge_manifest = storage.load_manifest(root / "DIVERGE")
diverge_month = next(iter(storage.manifest_months(diverge_manifest, "1d")))
diverge_file = storage.find_month_file(root, "DIVERGE", 2020, 1, "1d")
storage.write_month_file(diverge_file, [
    bar("2020-01-02", 100), bar("2020-01-03", 102),
])
diverge_report = audit.audit(root, ["DIVERGE"], now=FIXED_NOW)
diverge_events = [row for row in diverge_report["tickers"][0]["rows"]
                  if row["row_type"] == "event"]
check("event bars: file/manifest drift fails closed",
      len(diverge_events) == 1
      and diverge_events[0]["verdict"] == "UNVERIFIABLE"
      and "does not match" in diverge_events[0]["evidence"]["error"],
      str(diverge_events))

race_fp = seed_ticker(root, "RACE", 113, [
    bar("2020-01-02", 100), bar("2020-01-03", 101),
])
original_read = storage.read_month_file
mutated = [False]


def racing_read(path, *args, **kwargs):
    result = original_read(path, *args, **kwargs)
    if not mutated[0]:
        mutated[0] = True
        manifest = storage.load_manifest(root / "RACE")
        months = storage.manifest_months(manifest, "1d")
        first = next(iter(months))
        months[first]["mtime_ns"] += 1
        storage.save_manifest(root / "RACE", manifest)
    return result


storage.read_month_file = racing_read
try:
    race_rejected = False
    try:
        audit._strict_daily_bars(root, "RACE", race_fp)
    except audit.SplitAuditError as exc:
        race_rejected = "changed during" in str(exc)
finally:
    storage.read_month_file = original_read
check("event bars: manifest race is rejected after strict read", race_rejected)

# Candidate/correction decisions also recheck their consumed manifest state.
final_fp = seed_ticker(root, "FINAL", 115, common_bars)
write_cache(root, "FINAL", final_fp)
original_cache_load = cache.load_cache
final_mutated = [False]


def racing_cache_load(*args, **kwargs):
    result = original_cache_load(*args, **kwargs)
    if not final_mutated[0]:
        final_mutated[0] = True
        manifest = storage.load_manifest(root / "FINAL")
        months = storage.manifest_months(manifest, "1d")
        first = next(iter(months))
        months[first]["mtime_ns"] += 1
        storage.save_manifest(root / "FINAL", manifest)
    return result


cache.load_cache = racing_cache_load
try:
    final_report = audit.audit(root, ["FINAL"], now=FIXED_NOW)
finally:
    cache.load_cache = original_cache_load
final_rows = final_report["tickers"][0]["rows"]
check("fingerprint: candidate audit detects a concurrent manifest change",
      final_report["tickers"][0]["fingerprint"]["status"]
      == "changed_during_audit"
      and any(row["reason"] == "manifest_changed_during_audit"
              for row in final_rows)
      and not any(row["verdict"] == "CLEAN" for row in final_rows),
      str(final_rows))

# Archive evidence may enrich measurements but cannot invent missing history.
archive_only_root = base / "archive_only_bank"
archive_only_root.mkdir()
only_fp = seed_ticker(
    archive_only_root, "FTNT", 201, common_bars, ftnt_correction)
assert only_fp
archive_only = audit.audit(
    archive_only_root, ["FTNT"], archive_path=archive_path, now=FIXED_NOW)
check("archive: captured seam metrics cannot replace split history",
      archive_only["archived_verdict_counts"]["UNVERIFIABLE"] == 1
      and archive_only["archived_verdict_counts"]["PHANTOM"] == 0,
      str(archive_only["tickers"][0]["archived_rows"]))

bad_archive = base / "bad-archive.json"
bad_archive.write_text(json.dumps({"read_only": False, "rows": []}),
                       encoding="utf-8")
bad_rejected = False
try:
    audit.load_archive(bad_archive)
except audit.SplitAuditError:
    bad_rejected = True
check("archive: artifact not marked read-only is rejected", bad_rejected)

missing_manifest_root = base / "missing_manifest_bank"
missing_manifest_root.mkdir()
missing_manifest_report = audit.audit(
    missing_manifest_root, ["FTNT"], archive_path=archive_path,
    now=FIXED_NOW)
check("archive: missing current manifest cannot erase historical evidence",
      missing_manifest_report["verdict_counts"]["UNVERIFIABLE"] == 1
      and missing_manifest_report[
          "archived_verdict_counts"]["UNVERIFIABLE"] == 1,
      str(missing_manifest_report["tickers"][0]))

# Explicit write may replace only the report sidecar.
before_write = snapshot(root, exclude={audit.REPORT_FILE})
written = audit.audit(
    root, ["FTNT", "WBD", "NVDA"], archive_path=archive_path,
    now=FIXED_NOW, write=True)
report_path = root / audit.REPORT_FILE
after_write = snapshot(root, exclude={audit.REPORT_FILE})
on_disk = json.loads(report_path.read_text(encoding="utf-8"))
check("write: explicit mode creates one valid versioned report",
      written["written"] is True
      and on_disk["kind"] == audit.REPORT_KIND
      and on_disk["version"] == audit.REPORT_VERSION)
check("write: report sidecar is the only changed bank path",
      before_write == after_write
      and not list(root.glob(f"{audit.REPORT_FILE}.*.tmp")))

# Default CLI stays no-write and reports network=false.
cli_root = base / "cli_bank"
cli_root.mkdir()
cli_fp = seed_ticker(cli_root, "CLI", 301, common_bars)
write_cache(cli_root, "CLI", cli_fp)
cli_out = io.StringIO()
with contextlib.redirect_stdout(cli_out):
    cli_code = audit.main([
        "--root", str(cli_root), "--ticker", "CLI",
    ])
cli_payload = json.loads(cli_out.getvalue())
check("cli: default mode is offline and does not write report",
      cli_code == 0 and cli_payload["network"] is False
      and cli_payload["written"] is False
      and not (cli_root / audit.REPORT_FILE).exists())

# The project acceptance run supplies the actual ignored 2026-07-09 artifact.
if "--archive" in sys.argv:
    archive_index = sys.argv.index("--archive")
    try:
        actual_archive_path = Path(sys.argv[archive_index + 1])
    except IndexError as exc:
        raise SystemExit("--archive requires a path") from exc
    actual_archive = audit.load_archive(actual_archive_path)
    actual_tickers = {
        item["candidate"]["ticker"] for item in actual_archive["candidates"]}
    check("actual archive: 2026-07-09 artifact ingests without row errors",
          actual_archive["candidate_count"] > 2
          and not actual_archive["errors"], str(actual_archive["errors"]))
    check("actual archive: FTNT and WBD candidates are present",
          {"FTNT", "WBD"} <= actual_tickers, str(sorted(actual_tickers)))
    actual_acceptance = audit.audit(
        root, ["FTNT", "WBD", "NVDA"],
        archive_path=actual_archive_path, now=FIXED_NOW)
    actual_by_ticker = {
        row["ticker"]: row for row in actual_acceptance["tickers"]}
    check("actual archive: FTNT historical/current states stay distinct",
          actual_by_ticker["FTNT"]["archived_verdict_counts"]["PHANTOM"] == 1
          and actual_by_ticker["FTNT"]["verdict_counts"]["RESOLVED"] == 1,
          str(actual_by_ticker["FTNT"]))
    check("actual archive: WBD resolves and current severe gate stays zero",
          actual_by_ticker["WBD"]["verdict_counts"]["RESOLVED"] == 1
          and all(actual_acceptance["verdict_counts"][name] == 0
                  for name in ("PHANTOM", "MISSING", "REGRESSION")),
          str(actual_acceptance["verdict_counts"]))
else:
    print("[SKIP] 4 actual-archive checks (run with --archive PATH)")

print(f"\n{N[0]} checks, {len(FAILS)} failed")
if FAILS:
    print("FAILED: " + ", ".join(FAILS))
    raise SystemExit(1)
print("ALL PASS")
