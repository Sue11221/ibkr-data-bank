"""Deterministic adversarial tests for the export quality-note engine.

The suite uses only temporary manifests and injected Tier 0/WS7 evidence. It
does not export market data, contact a provider, or write to the real bank.
"""

from __future__ import annotations

import ast
import copy
import datetime as dt
import hashlib
import json
import shutil
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import export_quality as quality  # noqa: E402
import stock_storage as storage  # noqa: E402
import stock_validate as validate  # noqa: E402


FAILURES = []
COUNT = [0]
NOW = dt.datetime(
    2026, 7, 10, 17, 30, 45,
    tzinfo=dt.timezone(dt.timedelta(hours=-4)))


def check(name, condition, detail=""):
    COUNT[0] += 1
    print(f"[{'PASS' if condition else 'FAIL'}] {name}"
          + (f" -- {detail}" if detail and not condition else ""))
    if not condition:
        FAILURES.append(name)


def expect_error(name, fn):
    try:
        fn()
    except quality.ExportQualityError:
        check(name, True)
    except Exception as exc:  # noqa: BLE001
        check(name, False, f"wrong exception {type(exc).__name__}: {exc}")
    else:
        check(name, False, "did not raise")


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def write_json(path, payload, *, allow_nan=False):
    raw = json.dumps(
        payload, sort_keys=True, separators=(",", ":"),
        allow_nan=allow_nan).encode("utf-8")
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_bytes(raw)
    return raw


def seed_manifest(root, ticker, marker=0):
    ticker_dir = Path(root) / ticker
    ticker_dir.mkdir(parents=True, exist_ok=True)
    manifest = storage.new_manifest(ticker, ticker)
    manifest["conid"] = 100_000 + sum(ord(char) for char in ticker)
    manifest["fixture_marker"] = marker
    storage.save_manifest(ticker_dir, manifest)
    raw = (ticker_dir / storage.MANIFEST_NAME).read_bytes()
    return sha(raw)


def seed_interval(root, ticker, interval="1m", *, rows=2):
    ticker_dir = Path(root) / ticker
    ticker_dir.mkdir(parents=True, exist_ok=True)
    manifest = storage.load_manifest(ticker_dir)
    if not isinstance(manifest, dict):
        manifest = storage.new_manifest(ticker, ticker)
    digest = hashlib.sha256(f"{ticker}:{interval}".encode("ascii")).hexdigest()
    storage.manifest_months(manifest, interval)["2026-06"] = {
        "status": "present",
        "sha256": digest,
        "rows": rows,
        "first": "6/1/2026 9:30:00",
        "last": "6/30/2026 15:59:00",
    }
    storage.save_manifest(ticker_dir, manifest)
    return storage.interval_state_fingerprint(root, ticker, interval)


def manifest_sha(root, ticker):
    return sha((Path(root) / ticker / storage.MANIFEST_NAME).read_bytes())


def xval_entry(ticker, fingerprint, rows, *, interval="1m",
               status="discrepancy", observed_status=None,
               finished="2026-07-13T12:00:00-04:00", checked_days=252,
               level_shift_date=None, suspected_factor=None,
               full_field_counts=None, distinct_dates=None):
    field_counts = {}
    dates = set()
    normalized_rows = []
    for index, (day, field) in enumerate(rows):
        field_counts[field] = field_counts.get(field, 0) + 1
        dates.add(day)
        normalized_rows.append({
            "date": day,
            "field": field,
            "derived": 100.0 + index,
            "reference": 99.0 + index,
            "percent": 1.01,
        })
    if level_shift_date is not None:
        field_counts["level_shift"] = field_counts.get("level_shift", 0) + 1
        dates.add(level_shift_date)
        normalized_rows.append({
            "date": level_shift_date,
            "field": "level_shift",
            "derived": 1.0,
            "reference": 0.5,
            "percent": 100.0,
        })
    if full_field_counts is not None:
        effective_fields = dict(full_field_counts)
        if any(effective_fields.get(field, 0) < count
               for field, count in field_counts.items()):
            raise AssertionError("full field counts omit persisted evidence")
    else:
        effective_fields = field_counts
    row_count = sum(effective_fields.values())
    distinct_count = len(dates) if distinct_dates is None else distinct_dates
    coverage_days = max(1, checked_days)
    current = observed_status is None
    fp = {
        "schema_version": storage.INTERVAL_FINGERPRINT_VERSION,
        "algorithm": "sha256",
        "before_sha256": fingerprint["sha256"],
        "after_sha256": fingerprint["sha256"] if current else None,
        "sha256": fingerprint["sha256"] if current else None,
        "current": current,
        "present": fingerprint["present"],
        "backfill_incomplete": fingerprint["backfill_incomplete"],
        "month_count": fingerprint["month_count"],
        "verified_absent_count": fingerprint["verified_absent_count"],
    }
    if observed_status is not None:
        fp.update({
            "observed_status": observed_status,
            "observed_note": "fixture observed discrepancy",
            "error": "fixture provenance capture failed",
        })
    return {
        "schema_version": quality.XVAL_SCHEMA_VERSION,
        "ticker": ticker,
        "interval": interval,
        "status": status,
        "provider": "stockanalysis",
        "requested_range": "15Y",
        "reference_coverage": {
            "reference_first_date": "2026-06-01",
            "reference_last_date": "2026-06-30",
            "reference_day_count": coverage_days,
            "stored_first_date": "2026-06-01",
            "stored_last_date": "2026-06-30",
            "stored_day_count": coverage_days,
            "full_history_requested": False,
            "head_gap_days": 0,
            "head_tolerance_days":
                quality.XVAL_FULL_HISTORY_HEAD_TOLERANCE_DAYS,
            "full_history_head_ok": None,
        },
        "asof": finished,
        "started_at": "2026-07-13T11:59:00-04:00",
        "finished_at": finished,
        "score": None,
        "note": "fixture discrepancy",
        "interval_fingerprint": fp,
        "evidence_counts": {
            "row_count": row_count,
            "persisted_row_count": len(normalized_rows),
            "row_cap": quality.MAX_XVAL_ROWS,
            "truncated": row_count > len(normalized_rows),
            "distinct_dates": distinct_count,
            "field_counts": effective_fields,
        },
        "detail": {
            "checked": checked_days,
            "rows": normalized_rows,
            "level_shift": ({"date": level_shift_date, "factor": 2.0}
                            if level_shift_date is not None else None),
            "suspected_factor": suspected_factor,
        },
    }


def combined_entry(ticker, fingerprint, internal_fingerprint, day,
                   *, interval="1m"):
    digest = fingerprint["sha256"]
    internal_digest = internal_fingerprint["sha256"]
    return {
        "schema_version": quality.COMBINED_SCHEMA_VERSION,
        "ticker": ticker,
        "interval": interval,
        "internal_interval": quality.COMBINED_INTERNAL_INTERVAL,
        "source": "3-source",
        "status": "flagged",
        "asof": "2026-07-13T12:05:00-04:00",
        "started_at": "2026-07-13T12:04:00-04:00",
        "finished_at": "2026-07-13T12:05:00-04:00",
        "reference_coverage": {
            "external": {
                "first_date": "2026-06-01",
                "last_date": "2026-06-30",
                "day_count": 20,
                "derived_first_date": "2026-06-01",
                "derived_last_date": "2026-06-30",
                "derived_day_count": 20,
                "head_gap_days": 0,
                "head_tolerance_days":
                    quality.XVAL_FULL_HISTORY_HEAD_TOLERANCE_DAYS,
                "head_ok": True,
                "requested_range": "Max",
            },
            "internal": {
                "first_date": "2026-06-01",
                "last_date": "2026-06-30",
                "day_count": 20,
                "derived_first_date": "2026-06-01",
                "derived_last_date": "2026-06-30",
                "derived_day_count": 20,
                "head_gap_days": 0,
                "head_tolerance_days":
                    quality.XVAL_FULL_HISTORY_HEAD_TOLERANCE_DAYS,
                "head_ok": True,
                "requested_range": None,
            },
        },
        "candidates": [day],
        "persistent": [day],
        "cleared": [],
        "candidate_count": 1,
        "persistent_count": 1,
        "cleared_count": 0,
        "row_cap": quality.MAX_XVAL_ROWS,
        "truncated": False,
        "interval_fingerprint": {
            "schema_version": storage.INTERVAL_FINGERPRINT_VERSION,
            "algorithm": "sha256",
            "before_sha256": digest,
            "after_sha256": digest,
            "sha256": digest,
            "current": True,
            "present": fingerprint["present"],
            "backfill_incomplete": fingerprint["backfill_incomplete"],
            "month_count": fingerprint["month_count"],
            "verified_absent_count": fingerprint["verified_absent_count"],
        },
        "internal_interval_fingerprint": {
            "schema_version": storage.INTERVAL_FINGERPRINT_VERSION,
            "algorithm": "sha256",
            "before_sha256": internal_digest,
            "after_sha256": internal_digest,
            "sha256": internal_digest,
            "current": True,
            "present": internal_fingerprint["present"],
            "backfill_incomplete": internal_fingerprint[
                "backfill_incomplete"],
            "month_count": internal_fingerprint["month_count"],
            "verified_absent_count":
                internal_fingerprint["verified_absent_count"],
        },
    }


def ws7_row(ticker, fingerprint, verdict, **extra):
    return {
        "ticker": ticker,
        "manifest_fingerprint": fingerprint,
        "verdict": verdict,
        **extra,
    }


def write_ws7(path, rows, *, finished="2026-07-10T20:00:00+00:00",
              version=quality.WS7_VERSION, kind=quality.WS7_KIND,
              counts=None, allow_nan=False, **extra):
    computed = {verdict: 0 for verdict in quality.WS7_VERDICTS}
    for row in rows:
        verdict = row.get("verdict") if isinstance(row, dict) else None
        if verdict in computed:
            computed[verdict] += 1
    payload = {
        "kind": kind,
        "version": version,
        "report_only": True,
        "provider": "stockanalysis",
        "reference_range": "Max",
        "finished_at": finished,
        "params": {"ticker_count": len(rows)},
        "counts": computed if counts is None else counts,
        "rows": rows,
        **extra,
    }
    return write_json(path, payload, allow_nan=allow_nan)


def write_coverage(path, stale, *, current=0, unevaluated=None, version=None):
    unevaluated = list(unevaluated or [])
    payload = {
        "kind": "coverage_audit",
        "version": (quality.COVERAGE_SCHEMA_VERSION
                    if version is None else version),
        "report_only": True,
        "network": False,
        "asof": "2026-07-13T17:00:00",
        "primary_interval": quality.COVERAGE_PRIMARY_INTERVAL,
        "thresholds": {
            "kind_forward_stale_months": quality.COVERAGE_STALE_MONTHS,
        },
        "kind_series_count": current + len(stale) + len(unevaluated),
        "kind_current_count": current,
        "kind_forward_stale": stale,
        "kind_unevaluated": unevaluated,
    }
    return write_json(path, payload)


def coverage_row(ticker, kind="1d", *, kind_last="2017-02",
                 primary_last="2026-07"):
    start_ord = int(kind_last[:4]) * 12 + int(kind_last[5:7]) - 1
    end_ord = int(primary_last[:4]) * 12 + int(primary_last[5:7]) - 1
    return {
        "ticker": ticker,
        "kind": kind,
        "kind_last": kind_last,
        "primary_last": primary_last,
        "lag_months": end_ord - start_ord,
        "verdict": "KIND_FORWARD_STALE",
    }


def envelope(payload, fingerprint="a" * 64):
    return {
        **payload,
        "network": False,
        "written": False,
        "evidence": {
            "fingerprint": fingerprint,
            "paths": ["fixture-evidence.json"],
        },
    }


def split_status(ticker, rows, *, detection_current=True):
    return envelope({
        "kind": "tier0_ticker_status",
        "ticker": ticker,
        "current": {
            "split": {
                "detection_current": detection_current,
                "repair_queue": rows,
            },
        },
    })


class FakeTier0:
    def __init__(self, queue=(), details=None, *, fail_queue=False,
                 mutate=None, gaps=None, health_current=True):
        self.queue = list(queue)
        self.details = dict(details or {})
        self.gaps = dict(gaps or {})
        self.fail_queue = fail_queue
        self.mutate = mutate
        self.mutated = False
        self.health_current = bool(health_current)
        self.gap_calls = []

    def repair_queue(self, *, cursor=0, limit=100):
        if self.fail_queue:
            raise RuntimeError("fixture Tier 0 outage")
        if self.mutate is not None and not self.mutated:
            self.mutated = True
            self.mutate()
        values = self.queue if self.health_current else []
        return envelope({
            "kind": "tier0_repair_queue",
            "asof": "2026-07-10T17:00:00-04:00",
            "evidence_current": self.health_current,
            "queue": values,
            "pagination": {
                "cursor": cursor,
                "limit": limit,
                "returned": len(values),
                "next_cursor": None,
            },
        })

    def cached_gap_summary(self, *, ticker=None, interval=None,
                           cursor=0, limit=10):
        self.gap_calls.append((ticker, interval))
        value = self.gaps.get(ticker)
        if isinstance(value, Exception):
            raise value
        if value is None:
            value = {
                "ticker": ticker, "interval": interval, "current": True,
                "missing_total": 0, "gap_events": 0, "days": 0,
                "missing_days": 0, "missing_day_list": [],
                "missing_day_runs": [],
                "largest_missing_day_run": {
                    "count": 0, "start": None, "end": None},
                "source_absent": 0, "source_absent_list": [],
                "ignored": 0, "ignored_list": [],
                "asof": "2026-07-13T17:03:01-04:00",
                "interval_fingerprint": {
                    "current": True, "before_sha256": "b" * 64,
                    "after_sha256": "b" * 64},
            }
        current = value.get("current") is True
        return envelope({
            "kind": "tier0_cached_gap_summary",
            "asof": value.get("asof") or "2026-07-13T17:03:01-04:00",
            "filters": {"ticker": ticker, "interval": interval},
            "applicable": True,
            "evidence_current": current,
            "series": [copy.deepcopy(value)] if current else [],
            "unavailable": ([] if current else [{
                "ticker": ticker, "interval": interval,
                "state": value.get("state") or "stale",
                "reason": value.get("reason") or "fixture gap evidence is stale",
            }]),
            "errors": [],
            "pagination": {
                "cursor": cursor, "limit": limit,
                "returned": 1 if current else 0, "next_cursor": None,
            },
        })

    def ticker_status(self, ticker):
        value = self.details.get(ticker)
        if isinstance(value, Exception):
            raise value
        if value is None:
            raise RuntimeError(f"missing fixture detail for {ticker}")
        return copy.deepcopy(value)


def batch(destination, state="PARTIAL", *, interval="1d",
          start="2026-01-01", end="2026-07-10"):
    return {
        "run_id": "fixture-export-20260710",
        "format": "csv",
        "interval": interval,
        "sessions": "RTH",
        "start": start,
        "end": end,
        "destination": str(destination),
        "state": state,
    }


def written(ticker, rows=10, *, start=None, end=None, sources=None):
    result = {"status": "WRITTEN", "rows": rows, "file": f"{ticker}.csv"}
    if start is not None or end is not None:
        result.update({"start": start, "end": end})
    if sources is not None:
        result["source_intervals"] = list(sources)
    return result


def gap_status(ticker, *, interval="1d", missing=(), source_absent=(),
               ignored=(), runs=None, fingerprint="b" * 64):
    missing = sorted(missing)
    source_absent = sorted(source_absent)
    ignored = sorted(ignored)
    if runs is None:
        runs = ([] if not missing else [{
            "count": len(missing), "start": missing[0], "end": missing[-1],
        }])
    largest = max(
        runs, key=lambda item: item["count"],
        default={"count": 0, "start": None, "end": None})
    return {
        "ticker": ticker,
        "interval": interval,
        "current": True,
        "missing_total": len(missing),
        "gap_events": len(runs),
        "days": 120,
        "missing_days": len(missing),
        "missing_day_list": missing,
        "missing_day_runs": copy.deepcopy(runs),
        "largest_missing_day_run": copy.deepcopy(largest),
        "source_absent": len(source_absent),
        "source_absent_list": source_absent,
        "ignored": len(ignored),
        "ignored_list": ignored,
        "asof": "2026-07-13T17:03:01-04:00",
        "interval_fingerprint": {
            "current": True,
            "before_sha256": fingerprint,
            "after_sha256": fingerprint,
        },
    }


def tree_snapshot(root):
    result = []
    for path in sorted(item for item in Path(root).rglob("*") if item.is_file()):
        raw = path.read_bytes()
        stat = path.stat()
        result.append((
            path.relative_to(root).as_posix(), len(raw), stat.st_mtime_ns,
            sha(raw)))
    return result


def by_ticker(report):
    return {row["ticker"]: row for row in report["rows"]}


def run():
    project = Path(tempfile.mkdtemp(prefix="export-quality-selftest-"))
    bank = project / storage.STORAGE_DIR_NAME
    run_logs = project / "Run Logs"
    output = project / "Export Output"
    outside = project / "outside.json"
    for path in (bank, run_logs, output):
        path.mkdir(parents=True)

    health_bank = project / "Health Bank"
    health_bank.mkdir()
    seed_manifest(health_bank, "HEALTH")
    health_state = storage.bank_manifest_state_fingerprint(health_bank)
    cached_health = {
        "kind": "health_report",
        "marker": "cached",
        "source_state": {
            "schema_version": storage.BANK_STATE_FINGERPRINT_VERSION,
            "algorithm": "sha256",
            "before_sha256": health_state["sha256"],
            "after_sha256": health_state["sha256"],
            "manifest_count": health_state["manifest_count"],
            "current": True,
        },
    }
    quality.health_report.write_report(health_bank, cached_health)
    health_audits = []
    current_health = quality.ensure_current_health(
        health_bank,
        audit_fn=lambda root: health_audits.append(root) or {"marker": "new"})
    check("current cached health skips regeneration by default",
          current_health.get("marker") == "cached" and not health_audits)
    changed_manifest = storage.load_manifest(health_bank / "HEALTH")
    changed_manifest["fixture_marker"] = 1
    storage.save_manifest(health_bank / "HEALTH", changed_manifest)
    changed_health = quality.ensure_current_health(
        health_bank,
        audit_fn=lambda root: health_audits.append(root) or {"marker": "new"})
    check("manifest change triggers exactly one health regeneration",
          changed_health.get("marker") == "new"
          and health_audits == [health_bank.resolve()])

    selected = [
        "AAPL", "ABSENT", "BAD", "CANCEL", "CAND", "EMPTY",
        "FAIL", "HIST", "HOLES", "NOREF", "STALE", "UNV",
    ]
    present = [ticker for ticker in selected if ticker != "ABSENT"]
    fingerprints = {
        ticker: seed_manifest(bank, ticker)
        for ticker in present
    }
    long_reason = "provider overlap unavailable " + "x" * 1_000
    rows = [
        ws7_row("AAPL", fingerprints["AAPL"], "CLEAN"),
        ws7_row("BAD", fingerprints["BAD"], "CLEAN"),
        ws7_row("CANCEL", fingerprints["CANCEL"], "CLEAN"),
        ws7_row("CAND", fingerprints["CAND"], "SEAM_CANDIDATE"),
        ws7_row("EMPTY", fingerprints["EMPTY"], "CLEAN"),
        ws7_row("FAIL", fingerprints["FAIL"], "CLEAN"),
        ws7_row(
            "HIST", fingerprints["HIST"], "HISTORIC_BASIS_OFFSET",
            basis_actions_applied=[{
                "type": "basis_ratio", "date": "2020-01-02", "factor": 2.0,
            }]),
        ws7_row("HOLES", fingerprints["HOLES"], "CLEAN"),
        ws7_row("STALE", "0" * 64, "CURRENT_MISMATCH"),
        ws7_row("UNV", fingerprints["UNV"], "UNVERIFIABLE",
                 reason=long_reason),
    ]
    ws7_path = run_logs / "external-sweep-fixture-v2.json"
    ws7_raw = write_ws7(ws7_path, rows)
    bad_action = {
        "ticker": "BAD",
        "verdict": "PHANTOM",
        "scope": "current",
        "repair_queue": True,
        "reason": "Current split detector proves a phantom split candidate.",
        "recommended_action": "Review and apply the verified correction.",
        "date": "2020-02-03",
    }
    tier0 = FakeTier0(
        ["BAD"], {"BAD": split_status("BAD", [bad_action])})
    outcomes = {
        ticker: written(ticker)
        for ticker in present
    }
    outcomes.update({
        "CANCEL": {"status": "CANCELLED", "rows": 0},
        "EMPTY": {"status": "EMPTY", "rows": 0},
        "FAIL": {"status": "FAILED", "rows": 0, "error": "fixture failure"},
        "HIST": {"status": "FAILED", "rows": 0,
                 "error": "basis row export failed"},
        "HOLES": {
            "status": "MISSING_MONTHS", "rows": 5, "file": "HOLES.csv",
            "missing_months": ["2026-02", "2026-03"],
        },
    })

    try:
        report = quality.build_report(
            batch(output), selected, present, outcomes,
            storage_root=bank, run_logs_root=run_logs,
            ws7_paths=[ws7_path], tier0_adapter=tier0, now=NOW)
        indexed = by_ticker(report)

        check("report declares report-only/no-network/no-bank-write",
              report["report_only"] is True
              and report["network"] is False
              and report["bank_written"] is False)
        check("every selected ticker appears exactly once in sorted order",
              [row["ticker"] for row in report["rows"]] == sorted(selected)
              and len(indexed) == len(selected))
        check("WRITTEN status is preserved",
              indexed["AAPL"]["export_status"] == "WRITTEN")
        check("EMPTY status is preserved",
              indexed["EMPTY"]["export_status"] == "EMPTY")
        check("FAILED status is preserved",
              indexed["FAIL"]["export_status"] == "FAILED")
        check("CANCELLED status is preserved",
              indexed["CANCEL"]["export_status"] == "CANCELLED")
        check("absent selection becomes NOT_IN_BANK",
              indexed["ABSENT"]["export_status"] == "NOT_IN_BANK")
        check("missing months stay an additional WRITTEN warning",
              indexed["HOLES"]["export_status"] == "WRITTEN"
              and indexed["HOLES"]["export_warnings"] == [
                  "Missing months: 2026-02, 2026-03"])

        check("exact current detector row becomes CONFIRMED_BAD",
              indexed["BAD"]["quality"] == "CONFIRMED_BAD"
              and indexed["BAD"]["detector"] == "split_audit:PHANTOM"
              and bool(indexed["BAD"]["recommended_action"])
              and indexed["BAD"]["quality_source_fingerprint"] == "a" * 64
              and len(indexed["BAD"]["detector_row_fingerprint"] or "") == 64)
        check("WS7 candidate remains REVIEW_REQUIRED",
              indexed["CAND"]["quality"] == "REVIEW_REQUIRED")
        check("historic basis offset remains KNOWN_DIFFERENCE",
              indexed["HIST"]["quality"] == "KNOWN_DIFFERENCE")
        check("source uncertainty remains UNVERIFIABLE",
              indexed["UNV"]["quality"] == "UNVERIFIABLE")
        check("compatible current clean evidence becomes CLEAN",
              indexed["AAPL"]["quality"] == "CLEAN")
        check("stale and absent evidence are not treated as clean or bad",
              indexed["STALE"]["quality"] == "NO_CURRENT_EVIDENCE"
              and indexed["STALE"]["historical_verdict"] == "CURRENT_MISMATCH"
              and indexed["NOREF"]["quality"] == "NO_CURRENT_EVIDENCE"
              and indexed["ABSENT"]["quality"] == "NO_CURRENT_EVIDENCE")
        check("export and quality axes stay independent",
              indexed["HIST"]["export_status"] == "FAILED"
              and indexed["HIST"]["quality"] == "KNOWN_DIFFERENCE")
        check("quality reasons are bounded",
              0 < len(indexed["UNV"]["quality_reason"]) <= quality.MAX_REASON)
        check("summary counts all export states and warning rows",
              report["summary"]["export"] == {
                  "WRITTEN": 7, "EMPTY": 1, "FAILED": 2,
                  "CANCELLED": 1, "NOT_IN_BANK": 1,
                  "MISSING_MONTHS": 1, "ROWS": 65,
              })
        check("summary separates all quality classes",
              report["summary"]["quality"] == {
                  "CONFIRMED_BAD": 1, "REVIEW_REQUIRED": 1,
                  "KNOWN_DIFFERENCE": 1, "UNVERIFIABLE": 1,
                  "CLEAN": 5, "NO_CURRENT_EVIDENCE": 3,
              })
        check("current WS7 source hash and version are recorded",
              report["sources"]["ws7"] == [{
                  "basename": ws7_path.name,
                  "sha256": sha(ws7_raw),
                  "bytes": len(ws7_raw),
                  "finished_at": "2026-07-10T20:00:00+00:00",
                  "version": 2,
              }]
              and len(indexed["AAPL"]["ws7_row_fingerprint"] or "") == 64)

        note = quality.render_text(report)
        check("renderer is deterministic UTF-8 text",
              note == quality.render_text(report)
              and note.encode("utf-8").decode("utf-8") == note)
        check("headline names only confirmed bad tickers",
              "Confirmed bad tickers: BAD\n" in note
              and "Confirmed bad tickers: CAND" not in note)
        check("clean tickers are counted but not expanded",
              "Clean=5" in note and "\nAAPL:" not in note)
        check("renderer includes required sections and disclaimer",
              all(token in note for token in (
                  "CONFIRMED BAD - ACTION REQUIRED", "REVIEW REQUIRED",
                  "KNOWN DIFFERENCES", "UNVERIFIABLE OR STALE",
                  "GAP EVIDENCE", "EXPORT WARNINGS",
                  "Tier 0 ticker detail: BAD", "Tier 0 gap detail: AAPL",
                  "fingerprints=", "never authorizes an automatic fetch")))
        check("renderer does not expose a traceback",
              "Traceback" not in note and len(note.encode("utf-8")) <= quality.MAX_NOTE_BYTES)

        no_bad_report = copy.deepcopy(report)
        bad_row = next(row for row in no_bad_report["rows"]
                       if row["ticker"] == "BAD")
        bad_row["quality"] = "CLEAN"
        no_bad_report["summary"]["quality"]["CONFIRMED_BAD"] = 0
        no_bad_report["summary"]["quality"]["CLEAN"] += 1
        check("no-finding headline says none literally",
              "Confirmed bad tickers: none\n"
              in quality.render_text(no_bad_report))

        fillable_gap = gap_status(
            "AAPL",
            missing=["2026-02-02", "2026-02-03", "2026-02-04"],
            source_absent=["2026-03-02"])
        gap_report = quality.build_report(
            batch(output, start="2026-02-01", end="2026-03-31"),
            ["AAPL"], ["AAPL"], {"AAPL": written("AAPL")},
            storage_root=bank, run_logs_root=run_logs,
            ws7_paths=[ws7_path],
            tier0_adapter=FakeTier0(gaps={"AAPL": fillable_gap}), now=NOW)
        gap_aapl = by_ticker(gap_report)["AAPL"]
        gap_codes = [item["code"] for item in gap_aapl["gap_evidence"]]
        check("exact-range fillable gaps require review with largest run",
              gap_aapl["quality"] == "REVIEW_REQUIRED"
              and gap_aapl["evidence_code"] == "FILLABLE_DAY_GAP"
              and gap_aapl["gap_evidence"][0]["missing_days"] == 3
              and gap_aapl["gap_evidence"][0]["largest_run"] == 3)
        check("source-absent days remain separate informational evidence",
              gap_codes == ["FILLABLE_DAY_GAP", "SOURCE_ABSENT_DAY"]
              and gap_aapl["gap_evidence"][1]["verdict_affecting"] is False)
        gap_note = quality.render_text(gap_report)
        check("gap note renders count, run, examples, and source-absent context",
              "3 fillable trading day(s)" in gap_note
              and "largest consecutive expected-session run=3" in gap_note
              and "2026-02-02,2026-02-03,2026-02-04" in gap_note
              and "1 trading day(s) in the exported range are marked source-absent"
              in gap_note)

        outside_gap = quality.build_report(
            batch(output, start="2026-04-01", end="2026-05-31"),
            ["AAPL"], ["AAPL"], {"AAPL": written("AAPL")},
            storage_root=bank, run_logs_root=run_logs,
            ws7_paths=[ws7_path],
            tier0_adapter=FakeTier0(gaps={"AAPL": fillable_gap}), now=NOW)
        outside_aapl = by_ticker(outside_gap)["AAPL"]
        check("gap evidence outside the exact export range has no effect",
              outside_aapl["quality"] == "CLEAN"
              and outside_aapl["gap_evidence"] == [])

        absent_only = quality.build_report(
            batch(output, start="2026-02-01", end="2026-03-31"),
            ["AAPL"], ["AAPL"], {"AAPL": written("AAPL")},
            storage_root=bank, run_logs_root=run_logs,
            ws7_paths=[ws7_path], tier0_adapter=FakeTier0(gaps={
                "AAPL": gap_status(
                    "AAPL", source_absent=["2026-03-02"]),
            }), now=NOW)
        absent_aapl = by_ticker(absent_only)["AAPL"]
        check("source-absent-only evidence stays clean and visible",
              absent_aapl["quality"] == "CLEAN"
              and [item["code"] for item in absent_aapl["gap_evidence"]]
              == ["SOURCE_ABSENT_DAY"])

        stale_gap = quality.build_report(
            batch(output), ["AAPL"], ["AAPL"],
            {"AAPL": written("AAPL")}, storage_root=bank,
            run_logs_root=run_logs, ws7_paths=[ws7_path],
            tier0_adapter=FakeTier0(gaps={"AAPL": {
                "current": False,
                "state": "stale",
                "reason": "stored interval changed after the gap scan",
                "asof": "2026-07-13T17:03:01-04:00",
            }}), now=NOW)
        stale_gap_aapl = by_ticker(stale_gap)["AAPL"]
        check("stale gap evidence cannot support a clean export verdict",
              stale_gap_aapl["quality"] == "NO_CURRENT_EVIDENCE"
              and stale_gap_aapl["evidence_code"]
              == "GAP_EVIDENCE_NONCURRENT")

        malformed_gap = gap_status(
            "AAPL", missing=["2026-02-02", "2026-02-03", "2026-02-04"])
        malformed_gap["missing_days"] = 4
        malformed_report = quality.build_report(
            batch(output), ["AAPL"], ["AAPL"],
            {"AAPL": written("AAPL")}, storage_root=bank,
            run_logs_root=run_logs, ws7_paths=[ws7_path],
            tier0_adapter=FakeTier0(gaps={"AAPL": malformed_gap}), now=NOW)
        check("malformed gap counts fail closed per ticker",
              by_ticker(malformed_report)["AAPL"]["quality"]
              == "NO_CURRENT_EVIDENCE"
              and any(item.get("source") == "gap_evidence"
                      and item.get("ticker") == "AAPL"
                      for item in malformed_report["sources"]["errors"]))

        malformed_runs = gap_status(
            "AAPL", missing=["2026-02-02", "2026-02-03", "2026-02-04"])
        malformed_runs["missing_day_runs"][0]["end"] = "2026-02-05"
        malformed_runs["largest_missing_day_run"]["end"] = "2026-02-05"
        malformed_runs_report = quality.build_report(
            batch(output), ["AAPL"], ["AAPL"],
            {"AAPL": written("AAPL")}, storage_root=bank,
            run_logs_root=run_logs, ws7_paths=[ws7_path],
            tier0_adapter=FakeTier0(gaps={"AAPL": malformed_runs}), now=NOW)
        check("malformed gap run partitions fail closed per ticker",
              by_ticker(malformed_runs_report)["AAPL"]["quality"]
              == "NO_CURRENT_EVIDENCE"
              and any(item.get("source") == "gap_evidence"
                      and item.get("ticker") == "AAPL"
                      for item in malformed_runs_report["sources"]["errors"]))

        stale_health = quality.build_report(
            batch(output), ["AAPL"], ["AAPL"],
            {"AAPL": written("AAPL")}, storage_root=bank,
            run_logs_root=run_logs, ws7_paths=[ws7_path],
            tier0_adapter=FakeTier0(health_current=False), now=NOW)
        check("non-current Tier 0 health suppresses old clean evidence",
              by_ticker(stale_health)["AAPL"]["quality"]
              == "NO_CURRENT_EVIDENCE"
              and any(item.get("source") == "tier0"
                      for item in stale_health["sources"]["errors"]))

        derived_tier0 = FakeTier0(
            gaps={"AAPL": RuntimeError("derived interval queried gap evidence")})
        derived_report = quality.build_report(
            batch(output, interval="1m-iv"), ["AAPL"], ["AAPL"],
            {"AAPL": written("AAPL")}, storage_root=bank,
            run_logs_root=run_logs, ws7_paths=[ws7_path],
            tier0_adapter=derived_tier0, now=NOW)
        check("derived intervals bypass primary price gap evidence",
              by_ticker(derived_report)["AAPL"]["quality"] == "CLEAN"
              and by_ticker(derived_report)["AAPL"]["gap_evidence"] == []
              and derived_tier0.gap_calls == [])

        before_tree = tree_snapshot(bank)
        original_atomic = quality.storage._atomic_write_bytes
        atomic_called = [False]

        def forbidden_write(*_args, **_kwargs):
            atomic_called[0] = True
            raise AssertionError("build_report attempted a storage write")

        quality.storage._atomic_write_bytes = forbidden_write
        try:
            second = quality.build_report(
                batch(output), selected, present, outcomes,
                storage_root=bank, run_logs_root=run_logs,
                ws7_paths=[ws7_path], tier0_adapter=tier0, now=NOW)
        finally:
            quality.storage._atomic_write_bytes = original_atomic
        check("report construction invokes no storage writer",
              not atomic_called[0] and second["bank_written"] is False)
        check("report construction leaves bank bytes and metadata unchanged",
              tree_snapshot(bank) == before_tree)

        source_tree = ast.parse(Path(quality.__file__).read_text(encoding="utf-8"))
        imports = set()
        for node in ast.walk(source_tree):
            if isinstance(node, ast.Import):
                imports.update(alias.name.split(".")[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imports.add(node.module.split(".")[0])
        check("engine imports no network or process client",
              imports.isdisjoint({
                  "socket", "subprocess", "urllib", "requests", "http",
                  "ib_async",
              }))
        display_source = (
            Path(quality.__file__).parent.parent / "display_data.py"
        ).read_text(encoding="utf-8")
        revalidate_start = display_source.index(
            "    def _storage_xval_revalidate(")
        revalidate_end = display_source.index(
            "\n    def ", revalidate_start + 8)
        revalidate_source = display_source[revalidate_start:revalidate_end]
        check("GUI re-validation thread targets the defined worker body",
              "threading.Thread(target=_work_body, daemon=True,"
              in revalidate_source
              and "threading.Thread(target=_work, daemon=True,"
              not in revalidate_source)

        coverage_tickers = ["AFTER", "BEFORE", "COV", "OTHER"]
        coverage_fingerprints = {
            ticker: seed_manifest(bank, ticker)
            for ticker in coverage_tickers
        }
        coverage_ws7 = run_logs / "external-sweep-coverage-fixture-v2.json"
        write_ws7(coverage_ws7, [
            ws7_row(ticker, coverage_fingerprints[ticker], "CLEAN")
            for ticker in coverage_tickers
        ])
        coverage_rows = [
            coverage_row("AFTER"),
            coverage_row("BEFORE"),
            coverage_row("COV"),
            coverage_row("GONE"),
            coverage_row("OTHER", "1m-iv"),
        ]
        coverage_raw = write_coverage(
            bank / quality.COVERAGE_BASENAME, coverage_rows)
        coverage_outcomes = {
            "AFTER": written(
                "AFTER", start="2026-08-01", end="2026-09-30"),
            "BEFORE": written(
                "BEFORE", start="2016-01-01", end="2017-01-31"),
            "COV": written(
                "COV", start="2017-02-01", end="2026-07-10"),
            "OTHER": written(
                "OTHER", start="2017-02-01", end="2026-07-10"),
        }
        coverage_report = quality.build_report(
            batch(output), coverage_tickers, coverage_tickers,
            coverage_outcomes, storage_root=bank, run_logs_root=run_logs,
            ws7_paths=[coverage_ws7], tier0_adapter=FakeTier0(), now=NOW)
        coverage_index = by_ticker(coverage_report)
        check("per-kind warning uses inclusive affected-month overlap",
              coverage_index["COV"]["quality"] == "REVIEW_REQUIRED"
              and coverage_index["COV"]["export_start"] == "2017-02-01"
              and coverage_index["COV"]["coverage_evidence"][0]["code"]
              == "KIND_FORWARD_STALE",
              str(coverage_index["COV"]))
        check("per-kind warning excludes non-overlapping ranges",
              coverage_index["BEFORE"]["quality"] == "CLEAN"
              and coverage_index["AFTER"]["quality"] == "CLEAN")
        check("per-kind warning requires the exact export interval",
              coverage_index["OTHER"]["quality"] == "CLEAN"
              and coverage_index["OTHER"]["coverage_evidence"] == [])
        joined_report = quality.build_report(
            batch(output, interval="1m"), ["OTHER"], ["OTHER"],
            {"OTHER": written(
                "OTHER", start="2017-02-01", end="2026-07-10",
                sources=["1m", "1m-iv"])},
            storage_root=bank, run_logs_root=run_logs,
            ws7_paths=[coverage_ws7], tier0_adapter=FakeTier0(), now=NOW)
        joined_row = by_ticker(joined_report)["OTHER"]
        check("per-kind warning follows the exact joined IV source",
              joined_row["quality"] == "REVIEW_REQUIRED"
              and joined_row["coverage_evidence"][0]["interval"] == "1m-iv")
        unjoined_report = quality.build_report(
            batch(output, interval="1m"), ["OTHER"], ["OTHER"],
            {"OTHER": written(
                "OTHER", start="2017-02-01", end="2026-07-10",
                sources=["1m"])},
            storage_root=bank, run_logs_root=run_logs,
            ws7_paths=[coverage_ws7], tier0_adapter=FakeTier0(), now=NOW)
        check("an unjoined stale kind does not warn",
              by_ticker(unjoined_report)["OTHER"]["coverage_evidence"] == [])
        frontier_report = quality.build_report(
            batch(output), ["COV"], ["COV"],
            {"COV": written(
                "COV", start="2017-02-01", end="2017-02-28",
                sources=["1d"])}, storage_root=bank,
            run_logs_root=run_logs, ws7_paths=[coverage_ws7],
            tier0_adapter=FakeTier0(), now=NOW)
        frontier_finding = by_ticker(frontier_report)["COV"][
            "coverage_evidence"][0]
        check("frontier-month warning avoids a definite overlap claim",
              "source-frontier month" in frontier_finding["summary"]
              and "overlaps the affected months" not in frontier_finding["summary"])
        nonwritten_report = quality.build_report(
            batch(output, state="CANCELLED"), ["COV"], ["COV"],
            {"COV": {"status": "CANCELLED", "rows": 0}},
            storage_root=bank, run_logs_root=run_logs,
            ws7_paths=[coverage_ws7], tier0_adapter=FakeTier0(
                gaps={"COV": gap_status(
                    "COV", missing=("2026-06-03",))}), now=NOW)
        nonwritten_row = by_ticker(nonwritten_report)["COV"]
        check("non-written outcomes receive no range/output overlays",
              nonwritten_row["coverage_evidence"] == []
              and nonwritten_row["gap_evidence"] == [])
        absent_coverage = quality.build_report(
            batch(output), ["GONE"], [], {}, storage_root=bank,
            run_logs_root=run_logs, ws7_paths=[coverage_ws7],
            tier0_adapter=FakeTier0(), now=NOW)
        check("stale sidecar cannot promote a ticker absent from the bank",
              by_ticker(absent_coverage)["GONE"]["quality"]
              == "NO_CURRENT_EVIDENCE"
              and by_ticker(absent_coverage)["GONE"]["coverage_evidence"]
              == [])
        check("coverage sidecar metadata is bounded and hashed",
              coverage_report["sources"]["coverage_audit"]["sha256"]
              == sha(coverage_raw)
              and coverage_report["sources"]["coverage_audit"]
              ["selected_stale"] == 4)
        coverage_note = quality.render_text(coverage_report)
        check("note renders an explicit per-kind coverage warning",
              "PER-KIND COVERAGE WARNINGS" in coverage_note
              and "COV 1d [warning]" in coverage_note
              and "Coverage audit: _coverage_audit.json" in coverage_note)

        open_report = quality.build_report(
            batch(output, start="", end=""), ["COV"], ["COV"],
            {"COV": written("COV")}, storage_root=bank,
            run_logs_root=run_logs, ws7_paths=[coverage_ws7],
            tier0_adapter=FakeTier0(), now=NOW)
        check("blank Max boundaries are treated as open-ended",
              by_ticker(open_report)["COV"]["quality"]
              == "REVIEW_REQUIRED")

        malformed_rows = copy.deepcopy(coverage_rows)
        malformed_rows[0]["lag_months"] += 1
        write_coverage(bank / quality.COVERAGE_BASENAME, malformed_rows)
        malformed_coverage = quality.build_report(
            batch(output), ["COV"], ["COV"],
            {"COV": written(
                "COV", start="2017-02-01", end="2026-07-10")},
            storage_root=bank, run_logs_root=run_logs,
            ws7_paths=[coverage_ws7], tier0_adapter=FakeTier0(), now=NOW)
        check("malformed coverage sidecar fails closed without a verdict",
              by_ticker(malformed_coverage)["COV"]["quality"] == "CLEAN"
              and malformed_coverage["sources"]["coverage_audit"] is None
              and any(item.get("source") == "coverage_audit"
                      for item in malformed_coverage["sources"]["errors"]))
        write_coverage(bank / quality.COVERAGE_BASENAME, coverage_rows)

        newer_path = run_logs / "external-sweep-newer-valid.json"
        write_ws7(
            newer_path,
            [ws7_row("AAPL", fingerprints["AAPL"], "CURRENT_MISMATCH")],
            finished="2026-07-10T21:00:00+00:00")
        newest_report = quality.build_report(
            batch(output), ["AAPL"], ["AAPL"], {"AAPL": written("AAPL")},
            storage_root=bank, run_logs_root=run_logs,
            ws7_paths=[ws7_path, newer_path], tier0_adapter=FakeTier0(),
            now=NOW)
        check("newest schema-valid artifact wins per ticker",
              by_ticker(newest_report)["AAPL"]["quality"] == "REVIEW_REQUIRED"
              and by_ticker(newest_report)["AAPL"]["ws7_artifact"]
              == newer_path.name)

        old_path = run_logs / "external-sweep-historical-v1.json"
        write_ws7(old_path, [rows[0]], version=1)
        unrelated_path = run_logs / "external-sweep-live-proof-fixture.json"
        write_ws7(
            unrelated_path, [], kind="external_sweep_live_proof",
            version=1)
        discovered = quality._load_ws7(run_logs, None, ["AAPL"])
        check("historical WS7 v1 is context, not a corruption warning",
              any(item["basename"] == old_path.name
                  for item in discovered["historical"])
              and not any(old_path.name in error
                          for error in discovered["errors"]))
        check("non-report files from broad discovery are ignored",
              discovered["ignored_candidates"] == 1
              and not any(unrelated_path.name in error
                          for error in discovered["errors"]))
        explicit_unrelated = quality._load_ws7(
            run_logs, [unrelated_path], ["AAPL"])
        check("explicit non-report evidence fails closed",
              bool(explicit_unrelated["errors"])
              and explicit_unrelated["rows"] == {})

        duplicate_path = run_logs / "external-sweep-duplicate.json"
        write_ws7(duplicate_path, [rows[0], copy.deepcopy(rows[0])])
        duplicate = quality._load_ws7(run_logs, [duplicate_path], ["AAPL"])
        check("duplicate WS7 ticker rows fail closed",
              duplicate["rows"] == {} and bool(duplicate["errors"]))

        unknown_path = run_logs / "external-sweep-unknown-verdict.json"
        write_ws7(unknown_path, [{
            **rows[0], "verdict": "CERTAINLY_BAD",
        }])
        unknown = quality._load_ws7(run_logs, [unknown_path], ["AAPL"])
        check("unknown WS7 verdict fails closed",
              unknown["rows"] == {} and bool(unknown["errors"]))

        schema_path = run_logs / "external-sweep-unknown-schema.json"
        write_ws7(schema_path, [rows[0]], version=3)
        schema = quality._load_ws7(run_logs, [schema_path], ["AAPL"])
        check("unknown WS7 schema fails closed",
              schema["rows"] == {} and bool(schema["errors"]))

        count_path = run_logs / "external-sweep-bad-counts.json"
        write_ws7(
            count_path, [rows[0]],
            counts={verdict: 0 for verdict in quality.WS7_VERDICTS})
        bad_counts = quality._load_ws7(run_logs, [count_path], ["AAPL"])
        check("WS7 count mismatch fails closed",
              bad_counts["rows"] == {} and bool(bad_counts["errors"]))

        nan_path = run_logs / "external-sweep-nan.json"
        nan_row = {**rows[0], "current_ratio": float("nan")}
        write_ws7(nan_path, [nan_row], allow_nan=True)
        nonfinite = quality._load_ws7(run_logs, [nan_path], ["AAPL"])
        check("non-finite WS7 value fails closed",
              nonfinite["rows"] == {} and bool(nonfinite["errors"]))

        missing = quality._load_ws7(
            run_logs, [run_logs / "external-sweep-missing.json"], ["AAPL"])
        check("missing WS7 report fails closed",
              missing["rows"] == {} and bool(missing["errors"]))
        write_json(outside, {"kind": "not-evidence"})
        escaped = quality._load_ws7(run_logs, [outside], ["AAPL"])
        check("WS7 path outside Run Logs fails closed",
              escaped["rows"] == {} and bool(escaped["errors"]))

        broken_dir = bank / "BROKEN"
        broken_dir.mkdir()
        write_json(broken_dir / storage.MANIFEST_NAME, {
            "folder": "BROKEN",
            "intervals": {"1d": {"months": []}},
        })
        broken_report = quality.build_report(
            batch(output), ["BROKEN"], ["BROKEN"],
            {"BROKEN": written("BROKEN")}, storage_root=bank,
            run_logs_root=run_logs, ws7_paths=[ws7_path],
            tier0_adapter=FakeTier0(), now=NOW)
        broken = by_ticker(broken_report)["BROKEN"]
        check("persistently malformed manifest is unavailable, not a race",
              broken["quality"] == "NO_CURRENT_EVIDENCE"
              and broken["quality_source"] == "manifest"
              and broken["evidence_code"] is None
              and any(item.get("ticker") == "BROKEN"
                      for item in broken_report["sources"]["errors"]))

        outage_report = quality.build_report(
            batch(output), ["AAPL", "HIST"], ["AAPL", "HIST"],
            {"AAPL": written("AAPL"), "HIST": written("HIST")},
            storage_root=bank, run_logs_root=run_logs,
            ws7_paths=[ws7_path],
            tier0_adapter=FakeTier0(fail_queue=True), now=NOW)
        outage = by_ticker(outage_report)
        check("Tier 0 outage prevents a clean assertion",
              outage["AAPL"]["quality"] == "NO_CURRENT_EVIDENCE"
              and any(item["source"] == "tier0"
                      for item in outage_report["sources"]["errors"]))
        check("Tier 0 outage does not erase known WS7 difference",
              outage["HIST"]["quality"] == "KNOWN_DIFFERENCE")

        stale_detail = split_status(
            "BAD", [bad_action], detection_current=False)
        stale_detector = quality.build_report(
            batch(output), ["BAD"], ["BAD"], {"BAD": written("BAD")},
            storage_root=bank, run_logs_root=run_logs,
            ws7_paths=[ws7_path],
            tier0_adapter=FakeTier0(["BAD"], {"BAD": stale_detail}),
            now=NOW)
        check("stale detector row cannot become confirmed bad",
              by_ticker(stale_detector)["BAD"]["quality"]
              == "REVIEW_REQUIRED")

        duplicate_detail = split_status(
            "BAD", [bad_action, copy.deepcopy(bad_action)])
        duplicate_detector = quality.build_report(
            batch(output), ["BAD"], ["BAD"], {"BAD": written("BAD")},
            storage_root=bank, run_logs_root=run_logs,
            ws7_paths=[ws7_path],
            tier0_adapter=FakeTier0(["BAD"], {"BAD": duplicate_detail}),
            now=NOW)
        check("ambiguous duplicate detector rows require review",
              by_ticker(duplicate_detector)["BAD"]["quality"]
              == "REVIEW_REQUIRED")

        missing_detail = quality.build_report(
            batch(output), ["BAD"], ["BAD"], {"BAD": written("BAD")},
            storage_root=bank, run_logs_root=run_logs,
            ws7_paths=[ws7_path], tier0_adapter=FakeTier0(["BAD"]),
            now=NOW)
        check("bare repair queue membership requires review, not bad label",
              by_ticker(missing_detail)["BAD"]["quality"]
              == "REVIEW_REQUIRED"
              and any(item.get("ticker") == "BAD"
                      for item in missing_detail["sources"]["errors"]))

        absent_action = {**bad_action, "ticker": "ABSENT"}
        absent_queue = quality.build_report(
            batch(output), ["ABSENT"], [], {}, storage_root=bank,
            run_logs_root=run_logs, ws7_paths=[ws7_path],
            tier0_adapter=FakeTier0(
                ["ABSENT"], {
                    "ABSENT": split_status("ABSENT", [absent_action]),
                }),
            now=NOW)
        check("stale queue detail cannot mark an absent ticker bad",
              by_ticker(absent_queue)["ABSENT"]["quality"]
              == "NO_CURRENT_EVIDENCE"
              and by_ticker(absent_queue)["ABSENT"]["export_status"]
              == "NOT_IN_BANK")

        expect_error(
            "duplicate selected ticker is rejected",
            lambda: quality.build_report(
                batch(output), ["AAPL", "AAPL"], ["AAPL"],
                {"AAPL": written("AAPL")}, storage_root=bank,
                run_logs_root=run_logs, ws7_paths=[ws7_path],
                tier0_adapter=FakeTier0(), now=NOW))
        expect_error(
            "WRITTEN outcome requires a committed filename",
            lambda: quality.build_report(
                batch(output), ["AAPL"], ["AAPL"],
                {"AAPL": {"status": "WRITTEN", "rows": 1}},
                storage_root=bank, run_logs_root=run_logs,
                ws7_paths=[ws7_path], tier0_adapter=FakeTier0(), now=NOW))
        expect_error(
            "non-written outcome cannot claim a committed file",
            lambda: quality.build_report(
                batch(output), ["AAPL"], ["AAPL"],
                {"AAPL": {"status": "EMPTY", "rows": 0,
                          "file": "AAPL.csv"}},
                storage_root=bank, run_logs_root=run_logs,
                ws7_paths=[ws7_path], tier0_adapter=FakeTier0(), now=NOW))
        expect_error(
            "missing-month warning requires a written file",
            lambda: quality.build_report(
                batch(output), ["AAPL"], ["AAPL"],
                {"AAPL": {"status": "FAILED", "rows": 0,
                          "missing_months": ["2026-01"]}},
                storage_root=bank, run_logs_root=run_logs,
                ws7_paths=[ws7_path], tier0_adapter=FakeTier0(), now=NOW))
        expect_error(
            "fractional export row count is rejected",
            lambda: quality.build_report(
                batch(output), ["AAPL"], ["AAPL"],
                {"AAPL": {"status": "WRITTEN", "rows": 1.5,
                          "file": "AAPL.csv"}},
                storage_root=bank, run_logs_root=run_logs,
                ws7_paths=[ws7_path], tier0_adapter=FakeTier0(), now=NOW))
        expect_error(
            "incomplete per-ticker export range is rejected",
            lambda: quality.build_report(
                batch(output), ["AAPL"], ["AAPL"],
                {"AAPL": written("AAPL", start="2026-01-01")},
                storage_root=bank, run_logs_root=run_logs,
                ws7_paths=[ws7_path], tier0_adapter=FakeTier0(), now=NOW))
        expect_error(
            "reversed per-ticker export range is rejected",
            lambda: quality.build_report(
                batch(output), ["AAPL"], ["AAPL"],
                {"AAPL": written(
                    "AAPL", start="2026-02-01", end="2026-01-01")},
                storage_root=bank, run_logs_root=run_logs,
                ws7_paths=[ws7_path], tier0_adapter=FakeTier0(), now=NOW))
        expect_error(
            "invalid injected report clock is rejected",
            lambda: quality.build_report(
                batch(output), ["AAPL"], ["AAPL"],
                {"AAPL": written("AAPL")}, storage_root=bank,
                run_logs_root=run_logs, ws7_paths=[ws7_path],
                tier0_adapter=FakeTier0(), now="not-a-clock"))

        first_note = quality.write_report_note(
            report, output, storage_root=bank, now=NOW, pid=77, counter=1)
        first_raw = first_note.read_bytes()
        second_note = quality.write_report_note(
            report, output, storage_root=bank, now=NOW, pid=77, counter=1)
        check("atomic writer creates a unique UTF-8 note",
              first_note.name ==
              "EXPORT_DATA_QUALITY_20260710-173045-77-0001.txt"
              and first_raw.decode("utf-8") == note)
        check("name collision never overwrites an older note",
              second_note.name ==
              "EXPORT_DATA_QUALITY_20260710-173045-77-0002.txt"
              and first_note.read_bytes() == first_raw
              and second_note.read_bytes() == first_raw)
        check("successful note writes leave no sibling temp",
              not list(output.glob("*.tmp")))
        expect_error(
            "note writer refuses a destination inside the bank",
            lambda: quality.write_report_note(
                report, bank, storage_root=bank, now=NOW,
                pid=77, counter=10))
        expect_error(
            "note writer refuses a missing destination",
            lambda: quality.write_report_note(
                report, project / "missing-output", storage_root=bank,
                now=NOW, pid=77, counter=10))

        notes_before_failure = {
            path.name: path.read_bytes() for path in output.glob("*.txt")}
        original_atomic = quality.storage._atomic_write_bytes

        def fail_atomic(*_args, **_kwargs):
            raise PermissionError("fixture locked output")

        quality.storage._atomic_write_bytes = fail_atomic
        try:
            expect_error(
                "note write failure is normalized",
                lambda: quality.write_report_note(
                    report, output, storage_root=bank, now=NOW,
                    pid=77, counter=20))
        finally:
            quality.storage._atomic_write_bytes = original_atomic
        check("failed note write cleans reservation and preserves prior notes",
              notes_before_failure == {
                  path.name: path.read_bytes() for path in output.glob("*.txt")}
              and not list(output.glob("*.tmp")))

        opening_cases = {
            "BNY": ("2026-05-21", ["open", "low"]),
            "BR": ("2025-11-04", ["open"]),
            "CMI": ("2025-10-14", ["open", "low"]),
            "CTVA": ("2026-06-18", ["open"]),
            "TEL": ("2026-04-02", ["open"]),
        }
        audit_known_answers = {
            "COP": ("2012-04-27", ["low", "close"]),
            "GE": ("2024-04-02", ["high", "close"]),
            "FTV": ("2025-05-01", ["open", "high"]),
            "XOM": ("2023-01-24", ["open", "low"]),
            "ZTS": ("2023-01-24", ["open", "high", "low"]),
        }
        overlay_tickers = [
            "AAPL", "BNY", "BR", "CMI", "COP", "CTVA", "DIAGVAL",
            "FTV", "GE", "ISOLATED", "STALEX", "T0XVAL", "TEL",
            "VOLVAL", "XOM", "ZTS",
        ]
        interval_fingerprints = {
            ticker: seed_interval(bank, ticker)
            for ticker in overlay_tickers
        }
        internal_fingerprints = {
            ticker: seed_interval(
                bank, ticker, quality.COMBINED_INTERNAL_INTERVAL)
            for ticker in overlay_tickers
        }
        producer_bars = [
            (dt.datetime(2026, 6, 1, 9, 30) + dt.timedelta(minutes=index),
             100.0, 101.0, 99.0, 100.0, 1)
            for index in range(200)
        ]
        producer_reference = {
            "2026-06-01": (100.0, 101.0, 99.0, 100.0, 200),
        }
        producer_entry = validate._cross_validate_ticker_body(
            bank, "AAPL", "1m", read_fn=lambda *_args: producer_bars,
            ref_fn=lambda *_args, **_kwargs: producer_reference)
        producer_loaded = quality._validate_xval_entry(
            "AAPL 1m", producer_entry)
        check("consumer accepts the exact Checkpoint A producer envelope",
              producer_loaded["status"] == "validated"
              and producer_loaded["provenance"]["current"] is True
              and producer_loaded["reference_coverage"]
              ["reference_first_date"] == "2026-06-01",
              str((producer_entry.get("status"), producer_loaded)))
        missing_received_range = copy.deepcopy(producer_entry)
        missing_received_range.pop("reference_coverage")
        expect_error(
            "schema-v4 refuses a current verdict without received-range evidence",
            lambda: quality._validate_xval_entry(
                "AAPL 1m", missing_received_range))
        short_max = copy.deepcopy(producer_entry)
        short_max.update({
            "requested_range": "Max",
            "status": "inconclusive",
            "score": None,
            "note": "fixture short full-history response",
        })
        max_stored_first = dt.date(2025, 1, 2)
        max_reference_first = dt.date(2026, 6, 1)
        short_max["reference_coverage"] = {
            "reference_first_date": max_reference_first.isoformat(),
            "reference_last_date": "2026-06-01",
            "reference_day_count": 1,
            "stored_first_date": max_stored_first.isoformat(),
            "stored_last_date": "2026-06-01",
            "stored_day_count": 2,
            "full_history_requested": True,
            "head_gap_days": (max_reference_first - max_stored_first).days,
            "head_tolerance_days":
                quality.XVAL_FULL_HISTORY_HEAD_TOLERANCE_DAYS,
            "full_history_head_ok": False,
        }
        short_max_loaded = quality._validate_xval_entry(
            "AAPL 1m", short_max)
        check("schema-v4 accepts a short Max response only as inconclusive",
              short_max_loaded["status"] == "inconclusive"
              and short_max_loaded["reference_coverage"]
              ["full_history_head_ok"] is False)
        short_max["status"] = "validated"
        expect_error(
            "schema-v4 refuses a verdict from a short Max response",
            lambda: quality._validate_xval_entry("AAPL 1m", short_max))

        known_samples = {
            "ABT": "2013-01-02",
            "CAG": "2016-11-09",
            "COP": "2012-04-26",
            "EBAY": "2015-07-16",
            "FDX": "2026-06-01",
            "GE": "2023-01-04",
        }
        catalog_raw = "\n".join(
            f"{ticker} {day}" for ticker, day in
            sorted(quality.XVAL_KNOWN_CORPORATE_ACTIONS)).encode("ascii")
        check("reviewed corporate-action catalog has the exact 57-event boundary",
              len(quality.XVAL_KNOWN_CORPORATE_ACTIONS) == 57
              and sha(catalog_raw)
              == "2d22f660df820872e0619865c3bfe6a16cac4917ade18629d269336a3a7f1ce4"
              and all(item in quality.XVAL_KNOWN_CORPORATE_ACTIONS
                      for item in known_samples.items())
              and all(not any(ticker == excluded for ticker, _day in
                              quality.XVAL_KNOWN_CORPORATE_ACTIONS)
                      for excluded in ("APO", "JCI", "OKE")))
        known_evidence = {}
        for ticker, day in known_samples.items():
            entry = xval_entry(
                ticker, interval_fingerprints["AAPL"], [],
                level_shift_date=day)
            known_evidence[ticker] = quality._xval_evidence(
                quality._validate_xval_entry(f"{ticker} 1m", entry))
        check("known-answer corporate actions classify as known differences",
              all(item["code"] == "known_corporate_action_basis"
                  and item["impact"] == "known_difference"
                  and item["verdict_affecting"] is False
                  and item["known_corporate_action"] is True
                  for item in known_evidence.values()))
        known_results = {
            ticker: quality._apply_validation_overlay(
                {"quality": "CLEAN", "reason": "fixture clean",
                 "quality_source": "ws7"},
                [item], [],
                {"export_status": "WRITTEN", "source_intervals": ["1m"]})
            for ticker, item in known_evidence.items()
        }
        check("FDX ABT EBAY GE CAG COP land KNOWN_DIFFERENCE",
              all(item["quality"] == "KNOWN_DIFFERENCE"
                  and "cross_validation" in item["quality_source"]
                  for item in known_results.values()))
        same_day_combined = [{
            "interval": "1m", "_all_dates": ["2013-01-02"],
            "_emit": False,
        }]
        persistent_known = quality._apply_validation_overlay(
            {"quality": "CLEAN", "reason": "fixture clean",
             "quality_source": "ws7"}, [known_evidence["ABT"]],
            same_day_combined,
            {"export_status": "WRITTEN", "source_intervals": ["1m"]})
        check("three-source persistence overrides a cataloged known difference",
              persistent_known["quality"] == "REVIEW_REQUIRED"
              and persistent_known["cross_validation_evidence"][0]["code"]
              == "three_source_persistent_price"
              and persistent_known["cross_validation_evidence"][0]
              ["known_corporate_action"] is True
              and persistent_known["cross_validation_evidence"][0]
              ["known_difference"] is False)
        coincident_volume = quality._xval_evidence(
            quality._validate_xval_entry(
                "VOLPERSIST 1m", xval_entry(
                    "VOLPERSIST", interval_fingerprints["AAPL"],
                    [("2013-01-02", "volume")])))
        persistent_volume = quality._apply_validation_overlay(
            {"quality": "CLEAN", "reason": "fixture clean",
             "quality_source": "ws7"}, [coincident_volume],
            same_day_combined,
            {"export_status": "WRITTEN", "source_intervals": ["1m"]})
        check("three-source date coincidence does not promote volume-only evidence",
              persistent_volume["quality"] == "CLEAN"
              and persistent_volume["cross_validation_evidence"][0]
              ["three_source_persistent"] is False
              and persistent_volume["cross_validation_evidence"][0]
              ["verdict_affecting"] is False)
        preserved_unverifiable = quality._apply_validation_overlay(
            {"quality": "UNVERIFIABLE", "reason": "fixture source unavailable",
             "quality_source": "ws7"}, [known_evidence["FDX"]], [],
            {"export_status": "WRITTEN", "source_intervals": ["1m"]})
        check("known difference does not erase a stronger unverifiable base",
              preserved_unverifiable["quality"] == "UNVERIFIABLE"
              and preserved_unverifiable["cross_validation_evidence"][0]
              ["known_difference"] is True)

        wrong_day = xval_entry(
            "ABT", interval_fingerprints["AAPL"], [],
            level_shift_date="2013-01-03")
        wrong_day_evidence = quality._xval_evidence(
            quality._validate_xval_entry("ABT 1m", wrong_day))
        missing_factor = xval_entry(
            "ABT", interval_fingerprints["AAPL"], [],
            level_shift_date="2013-01-02")
        missing_factor["detail"]["level_shift"].pop("factor")
        missing_factor_evidence = quality._xval_evidence(
            quality._validate_xval_entry("ABT 1m", missing_factor))
        factor_evidence = quality._xval_evidence(
            quality._validate_xval_entry(
                "FACTOR 1m", xval_entry(
                    "FACTOR", interval_fingerprints["AAPL"], [],
                    suspected_factor=4.0)))
        check("unknown dates and undated suspected factors fail closed to review",
              wrong_day_evidence["code"] == "unexplained_price_basis"
              and wrong_day_evidence["verdict_affecting"] is True
              and missing_factor_evidence["code"]
              == "unexplained_price_basis"
              and missing_factor_evidence["verdict_affecting"] is True
              and factor_evidence["code"] == "unexplained_price_basis"
              and factor_evidence["verdict_affecting"] is True)
        inconsistent_shift = xval_entry(
            "ABT", interval_fingerprints["AAPL"], [],
            level_shift_date="2013-01-02")
        inconsistent_shift["detail"]["level_shift"] = None
        expect_error(
            "level-shift detail must agree with full evidence counts",
            lambda: quality._validate_xval_entry(
                "ABT 1m", inconsistent_shift))

        density_at_boundary = xval_entry(
            "DENSE20", interval_fingerprints["AAPL"],
            [("2026-06-01", "high")], checked_days=1000,
            full_field_counts={"high": 20}, distinct_dates=20)
        density_above_boundary = xval_entry(
            "DENSE21", interval_fingerprints["AAPL"],
            [("2026-06-01", "high")], checked_days=1000,
            full_field_counts={"high": 21}, distinct_dates=21)
        short_dense = xval_entry(
            "HONA", interval_fingerprints["AAPL"],
            [("2026-06-01", "high"), ("2026-06-02", "low")],
            checked_days=14)
        boundary_evidence = quality._xval_evidence(
            quality._validate_xval_entry("DENSE20 1m", density_at_boundary))
        above_evidence = quality._xval_evidence(
            quality._validate_xval_entry("DENSE21 1m", density_above_boundary))
        short_evidence = quality._xval_evidence(
            quality._validate_xval_entry("HONA 1m", short_dense))
        check("density gate is full-count based and strictly greater than 2 percent",
              boundary_evidence["price_row_count"] == 20
              and boundary_evidence["verdict_affecting"] is False
              and above_evidence["price_row_count"] == 21
              and above_evidence["dense_price_evidence"] is True
              and above_evidence["verdict_affecting"] is True)
        check("density gate requires one trading year of checked coverage",
              short_evidence["price_row_density_pct"] > 2
              and short_evidence["density_eligible"] is False
              and short_evidence["verdict_affecting"] is False)
        class_c_specs = {
            "NVDA": None,
            "JCI": "2012-07-24",
            "OKE": "2020-03-10",
            "APO": "2020-03-13",
        }
        class_c_evidence = {}
        for ticker, shift_day in class_c_specs.items():
            full_counts = {"high": 30}
            if shift_day is not None:
                full_counts["level_shift"] = 1
            entry = xval_entry(
                ticker, interval_fingerprints["AAPL"],
                [("2026-06-01", "high")], checked_days=1000,
                level_shift_date=shift_day,
                full_field_counts=full_counts, distinct_dates=30)
            class_c_evidence[ticker] = quality._xval_evidence(
                quality._validate_xval_entry(f"{ticker} 1m", entry))
        check("NVDA JCI OKE APO remain the bounded dense review set",
              all(item["dense_price_evidence"] is True
                  and item["verdict_affecting"] is True
                  and item["impact"] == "review"
                  for item in class_c_evidence.values())
              and class_c_evidence["NVDA"]["code"]
              == "dense_price_discrepancy"
              and all(class_c_evidence[ticker]["code"]
                      == "unexplained_price_basis"
                      for ticker in ("JCI", "OKE", "APO")))
        missing_checked = copy.deepcopy(density_above_boundary)
        missing_checked["detail"].pop("checked")
        expect_error(
            "price evidence without checked-day coverage fails closed",
            lambda: quality._validate_xval_entry(
                "DENSE21 1m", missing_checked))
        excessive_checked = copy.deepcopy(density_above_boundary)
        excessive_checked["reference_coverage"]["reference_day_count"] = 999
        expect_error(
            "checked-day count cannot exceed received reference coverage",
            lambda: quality._validate_xval_entry(
                "DENSE21 1m", excessive_checked))

        producer_coverage = copy.deepcopy(combined_entry(
            "BNY", interval_fingerprints["BNY"],
            internal_fingerprints["BNY"],
            opening_cases["BNY"][0])["reference_coverage"])
        combined_producer_entry = validate.combined_crosscheck_with_provenance(
            bank, "BNY", "1m", lambda: {
                "status": "flagged",
                "candidates": [opening_cases["BNY"][0]],
                "persistent": [opening_cases["BNY"][0]],
                "cleared": [],
                "reference_coverage": copy.deepcopy(producer_coverage),
            })
        combined_producer_loaded = quality._validate_combined_entry(
            "BNY 1m", combined_producer_entry)
        check("consumer accepts the exact combined producer envelope",
              combined_producer_loaded["status"] == "flagged"
              and combined_producer_loaded["provenance"]["current"] is True
              and combined_producer_loaded["internal_provenance"]["current"]
              is True,
              str((combined_producer_entry.get("status"),
                   combined_producer_loaded)))
        missing_coverage_producer = (
            validate.combined_crosscheck_with_provenance(
                bank, "BNY", "1m", lambda: {
                    "status": "flagged",
                    "candidates": [opening_cases["BNY"][0]],
                    "persistent": [opening_cases["BNY"][0]],
                    "cleared": [],
                }))
        check("producer fails closed on a flagged result without coverage",
              missing_coverage_producer["status"] == "error"
              and not missing_coverage_producer["persistent"])
        mass_days = [
            (dt.date(2024, 1, 1) + dt.timedelta(days=index)).isoformat()
            for index in range(quality.MAX_XVAL_ROWS + 20)
        ]
        mass_entry = validate.combined_crosscheck_with_provenance(
            bank, "BNY", "1m", lambda: {
                "status": "flagged", "candidates": mass_days,
                "persistent": mass_days, "cleared": [],
                "reference_coverage": copy.deepcopy(producer_coverage),
                "unbounded": "x" * 1_000_000,
            })
        mass_loaded = quality._validate_combined_entry("BNY 1m", mass_entry)
        check("consumer accepts bounded mass combined evidence with full counts",
              mass_loaded["persistent_count"] == len(mass_days)
              and len(mass_loaded["persistent"]) == quality.MAX_XVAL_ROWS
              and mass_loaded["truncated"] is True
              and "unbounded" not in mass_entry)
        single_dependency = copy.deepcopy(combined_producer_entry)
        single_dependency.pop("internal_interval_fingerprint")
        expect_error(
            "combined schema-v3 refuses a missing stored-1d dependency",
            lambda: quality._validate_combined_entry(
                "BNY 1m", single_dependency))
        missing_combined_coverage = copy.deepcopy(combined_producer_entry)
        missing_combined_coverage.pop("reference_coverage")
        expect_error(
            "combined schema-v3 refuses schema-2-shaped coverage omission",
            lambda: quality._validate_combined_entry(
                "BNY 1m", missing_combined_coverage))
        null_flagged_coverage = copy.deepcopy(combined_producer_entry)
        null_flagged_coverage["reference_coverage"] = {
            "external": None, "internal": None,
        }
        expect_error(
            "combined schema-v3 flagged verdict requires dual coverage",
            lambda: quality._validate_combined_entry(
                "BNY 1m", null_flagged_coverage))
        flagged_coverage_error = copy.deepcopy(combined_producer_entry)
        flagged_coverage_error["reference_coverage_error"] = "fixture error"
        expect_error(
            "combined schema-v3 flagged verdict rejects coverage errors",
            lambda: quality._validate_combined_entry(
                "BNY 1m", flagged_coverage_error))
        missing_combined_time = copy.deepcopy(combined_producer_entry)
        missing_combined_time.pop("started_at")
        expect_error(
            "combined schema-v3 requires its producer timing envelope",
            lambda: quality._validate_combined_entry(
                "BNY 1m", missing_combined_time))
        malformed_combined_coverage = copy.deepcopy(combined_producer_entry)
        malformed_combined_coverage["reference_coverage"]["external"] = {
            "first_date": "2026-06-10",
            "last_date": "2026-06-30",
            "day_count": 15,
            "derived_first_date": "2026-06-01",
            "derived_last_date": "2026-06-30",
            "derived_day_count": 20,
            "head_gap_days": 0,
            "head_tolerance_days":
                quality.XVAL_FULL_HISTORY_HEAD_TOLERANCE_DAYS,
            "head_ok": True,
            "requested_range": "Max",
        }
        expect_error(
            "combined schema-v3 validates reference head-gap arithmetic",
            lambda: quality._validate_combined_entry(
                "BNY 1m", malformed_combined_coverage))
        excessive_combined_count = copy.deepcopy(combined_entry(
            "BNY", interval_fingerprints["BNY"],
            internal_fingerprints["BNY"], opening_cases["BNY"][0]))
        excessive_combined_count["reference_coverage"]["external"][
            "day_count"] = 31
        expect_error(
            "combined schema-v3 bounds coverage counts by date span",
            lambda: quality._validate_combined_entry(
                "BNY 1m", excessive_combined_count))
        bounded_range = copy.deepcopy(combined_producer_entry)
        bounded_range["status"] = "inconclusive"
        bounded_range["reference_coverage"]["external"].update({
            "requested_range": "5Y", "head_ok": None,
        })
        bounded_range_loaded = quality._validate_combined_entry(
            "BNY 1m", bounded_range)
        check("combined schema-v3 accepts non-Max inconclusive head metadata",
              bounded_range_loaded["status"] == "inconclusive"
              and bounded_range_loaded["reference_coverage"]["external"]
              ["head_ok"] is None)
        combined_race = combined_entry(
            "BNY", interval_fingerprints["BNY"],
            internal_fingerprints["BNY"], opening_cases["BNY"][0])
        combined_race["status"] = "inconclusive"
        combined_race["interval_fingerprint"].update({
            "after_sha256": "b" * 64,
            "sha256": None,
            "current": False,
            "observed_status": "flagged",
            "observed_note": "fixture observed flag",
            "error": "interval changed during combined validation",
        })
        combined_race_loaded = quality._validate_combined_entry(
            "BNY 1m", combined_race)
        check("consumer accepts a non-current combined diagnosis as ignored context",
              combined_race_loaded["status"] == "inconclusive"
              and combined_race_loaded["persistent"]
              == [opening_cases["BNY"][0]]
              and combined_race_loaded["provenance"]["current"] is False)
        seed_interval(bank, "STRUCTVAL", "1m-iv")
        ratio_bars = [
            (dt.datetime(2026, 6, 1, 9, 30), 6.0, 6.0, 6.0, 6.0, 1),
        ]
        structural_entry = validate._cross_validate_ticker_body(
            bank, "STRUCTVAL", "1m-iv",
            read_fn=lambda *_args: ratio_bars)
        structural_loaded = quality._validate_xval_entry(
            "STRUCTVAL 1m-iv", structural_entry)
        check("consumer accepts current structural producer rows as context",
              structural_loaded["status"] == "structural-flag"
              and structural_loaded["provider"] == "internal-structural")
        write_json(bank / quality.XVAL_BASENAME, {
            "STRUCTVAL 1m-iv": structural_entry,
        })
        structural_current = quality._load_cross_validation(
            bank, ["STRUCTVAL"])
        seed_interval(bank, "STRUCTVAL", "1d-iv")
        structural_stale = quality._load_cross_validation(
            bank, ["STRUCTVAL"])
        check("consumer rechecks the optional stored-daily ratio dependency",
              structural_current["source"]["current_entries"] == 1
              and structural_stale["source"]["current_entries"] == 0
              and structural_stale["source"]["ignored_stale"] == 1)
        xval_payload = {
            "AAPL 1m": xval_entry(
                "AAPL", interval_fingerprints["AAPL"],
                [("2026-06-03", "high"), ("2026-06-04", "close")]),
            "VOLVAL 1m": xval_entry(
                "VOLVAL", interval_fingerprints["VOLVAL"],
                [("2026-06-05", "volume")]),
            "DIAGVAL 1m": xval_entry(
                "DIAGVAL", interval_fingerprints["DIAGVAL"],
                [("2026-06-06", "close")], status="inconclusive",
                observed_status="discrepancy"),
            "T0XVAL 1m": xval_entry(
                "T0XVAL", interval_fingerprints["T0XVAL"],
                [("2026-06-07", "close")]),
            "ISOLATED 1m": xval_entry(
                "ISOLATED", interval_fingerprints["ISOLATED"],
                [("2026-06-08", "high")]),
            "STALEX 1m": xval_entry(
                "STALEX", interval_fingerprints["STALEX"],
                [("2026-06-09", "low")]),
            "LEGACY": {
                "ticker": "LEGACY", "status": "discrepancy",
                "asof": "2026-07-06T12:00:00",
            },
        }
        for ticker, (day, fields) in opening_cases.items():
            xval_payload[f"{ticker} 1m"] = xval_entry(
                ticker, interval_fingerprints[ticker],
                [(day, field) for field in fields])
        for ticker, (day, fields) in audit_known_answers.items():
            xval_payload[f"{ticker} 1m"] = xval_entry(
                ticker, interval_fingerprints[ticker],
                [(day, field) for field in fields])

        # Exact interval isolation: this must not stale ISOLATED's 1m evidence.
        seed_interval(bank, "ISOLATED", "1d", rows=3)
        # Exact interval mutation: this must stale STALEX's captured 1m evidence.
        seed_interval(bank, "STALEX", "1m", rows=3)

        xval_raw = write_json(bank / quality.XVAL_BASENAME, xval_payload)
        combined_payload = {
            f"{ticker} 1m": combined_entry(
                ticker, interval_fingerprints[ticker],
                internal_fingerprints[ticker], day)
            for ticker, (day, _fields) in opening_cases.items()
        }
        combined_payload["VOLVAL 1m"] = combined_entry(
            "VOLVAL", interval_fingerprints["VOLVAL"],
            internal_fingerprints["VOLVAL"], "2026-06-05")
        combined_payload["VOLVAL 1m"].update({
            "status": "ok", "candidates": [], "persistent": [],
            "candidate_count": 0, "persistent_count": 0,
        })
        combined_payload["DIAGVAL 1m"] = combined_entry(
            "DIAGVAL", interval_fingerprints["DIAGVAL"],
            internal_fingerprints["DIAGVAL"], "2026-06-06")
        combined_payload["DIAGVAL 1m"].update({
            "status": "error", "candidates": [], "persistent": [],
            "candidate_count": 0, "persistent_count": 0,
        })
        combined_payload["AAPL 1m"] = combined_entry(
            "AAPL", interval_fingerprints["AAPL"],
            internal_fingerprints["AAPL"], "2026-06-08")
        combined_payload["AAPL 1m"].update({
            "status": "inconclusive", "candidates": [], "persistent": [],
            "candidate_count": 0, "persistent_count": 0,
        })
        combined_payload["ISOLATED 1m"] = combined_entry(
            "ISOLATED", interval_fingerprints["ISOLATED"],
            internal_fingerprints["ISOLATED"], "2026-06-08")
        combined_raw = write_json(
            bank / quality.COMBINED_BASENAME, combined_payload)
        overlay_ws7 = run_logs / "external-sweep-overlay-fixture-v2.json"
        write_ws7(overlay_ws7, [
            ws7_row(ticker, manifest_sha(bank, ticker), "CLEAN")
            for ticker in overlay_tickers
        ], finished="2026-07-13T16:10:00+00:00")
        t0_action = {
            **bad_action,
            "ticker": "T0XVAL",
            "reason": "Current Tier 0 evidence remains authoritative.",
        }
        overlay_report = quality.build_report(
            batch(output, interval="1m"), overlay_tickers, overlay_tickers,
            {ticker: written(ticker) for ticker in overlay_tickers},
            storage_root=bank, run_logs_root=run_logs,
            ws7_paths=[overlay_ws7],
            tier0_adapter=FakeTier0(
                ["T0XVAL"], {
                    "T0XVAL": split_status("T0XVAL", [t0_action]),
                }),
            now=NOW)
        overlay = by_ticker(overlay_report)
        aapl_evidence = overlay["AAPL"]["cross_validation_evidence"][0]
        check("sparse OHLC evidence keeps clean AAPL informational",
              overlay["AAPL"]["quality"] == "CLEAN"
              and aapl_evidence["fields"] == ["high", "close"]
              and aapl_evidence["distinct_days"] == 2
              and aapl_evidence["example_dates"]
              == ["2026-06-03", "2026-06-04"]
              and aapl_evidence["asof"]
              == "2026-07-13T12:00:00-04:00"
              and aapl_evidence["code"] == "sparse_price_discrepancy"
              and aapl_evidence["verdict_affecting"] is False)
        check("additive findings preserve the base WS7 evidence",
              any(item.get("source") == "ws7"
                  for item in overlay["AAPL"]["quality_findings"])
              and any(item.get("source") == "cross_validation"
                      for item in overlay["AAPL"]["quality_findings"]))
        check("volume-only cross-validation remains informational",
              overlay["VOLVAL"]["quality"] == "CLEAN"
              and overlay["VOLVAL"]["cross_validation_evidence"][0]["code"]
              == "volume_only_discrepancy"
              and overlay["VOLVAL"]["cross_validation_evidence"][0]
              ["three_source_checked"] is True
              and overlay["VOLVAL"]["cross_validation_evidence"][0]
              ["three_source_persistent"] is False)
        check("non-current observed discrepancy stays visible but non-actionable",
              overlay["DIAGVAL"]["quality"] == "CLEAN"
              and overlay["DIAGVAL"]["cross_validation_evidence"][0]["code"]
              == "noncurrent_observed_discrepancy"
              and overlay["DIAGVAL"]["cross_validation_evidence"][0]
              ["verdict_affecting"] is False)
        check("five persistent opening-reference fixtures require review, not bad",
              all(overlay[ticker]["quality"] == "REVIEW_REQUIRED"
                  and overlay[ticker]["quality"] != "CONFIRMED_BAD"
                  and overlay[ticker]["cross_validation_evidence"][0]
                  ["auction_candidate"] is True
                   and overlay[ticker]["cross_validation_evidence"][0]
                   ["three_source_persistent"] is True
                   and overlay[ticker]["cross_validation_evidence"][0]["code"]
                   == "three_source_persistent_price"
                   for ticker in opening_cases))
        check("isolated Run 2 OHLC shapes remain visible but informational",
              all(overlay[ticker]["quality"] == "CLEAN"
                   and overlay[ticker]["quality"] != "CONFIRMED_BAD"
                   and overlay[ticker]["cross_validation_evidence"][0]
                   ["verdict_affecting"] is False
                   for ticker in audit_known_answers)
              and overlay["FTV"]["cross_validation_evidence"][0]
              ["auction_candidate"] is False
              and overlay["XOM"]["cross_validation_evidence"][0]
              ["auction_candidate"] is True)
        check("Tier 0 confirmed-bad precedence survives the additive overlay",
              overlay["T0XVAL"]["quality"] == "CONFIRMED_BAD"
              and any(item.get("source") == "cross_validation"
                      for item in overlay["T0XVAL"]
                      ["cross_validation_evidence"]))
        check("another interval does not stale sparse exact-series evidence",
              overlay["ISOLATED"]["quality"] == "CLEAN"
              and overlay["ISOLATED"]["cross_validation_evidence"][0]
              ["code"] == "sparse_price_discrepancy")
        check("selected interval mutation makes captured validation stale",
              overlay["STALEX"]["quality"] == "CLEAN"
              and overlay["STALEX"]["cross_validation_evidence"] == [])
        check("overlay summary preserves all precedence classes",
              overlay_report["summary"]["quality"] == {
                  "CONFIRMED_BAD": 1, "REVIEW_REQUIRED": 5,
                  "KNOWN_DIFFERENCE": 0, "UNVERIFIABLE": 0,
                  "CLEAN": 10, "NO_CURRENT_EVIDENCE": 0,
              })
        check("sidecar footer metadata is hashed, versioned, and diagnostic",
              overlay_report["sources"]["cross_validation"]["sha256"]
              == sha(xval_raw)
              and overlay_report["sources"]["cross_validation"]
              ["schema_version"] == quality.XVAL_SCHEMA_VERSION
              and overlay_report["sources"]["cross_validation"]
              ["ignored_legacy"] == 1
              and overlay_report["sources"]["cross_validation"]
              ["ignored_stale"] == 2
              and overlay_report["sources"]["cross_validation"]
              ["current_price_review"] == 0
              and overlay_report["sources"]["cross_validation"]
              ["current_price_informational"] == 13
              and overlay_report["sources"]["combined_flags"]["sha256"]
              == sha(combined_raw)
              and overlay_report["sources"]["combined_flags"]
              ["current_entries"] == 8
              and overlay_report["sources"]["combined_flags"]
              ["current_ok"] == 1
              and overlay_report["sources"]["combined_flags"]
              ["current_flagged"] == 5
              and overlay_report["sources"]["combined_flags"]
              ["current_error"] == 1
              and overlay_report["sources"]["combined_flags"]
              ["current_inconclusive"] == 1)
        check("stored-1d mutation stales combined evidence only",
              overlay_report["sources"]["combined_flags"]
              ["ignored_stale"] == 1
              and overlay["ISOLATED"]["cross_validation_evidence"][0]
              ["three_source_checked"] is False)
        overlay_note = quality.render_text(overlay_report)
        check("note renders fields, days, examples, as-of, diagnostics, and persistence",
              all(token in overlay_note for token in (
                  "CROSS-VALIDATION EVIDENCE",
                  "AAPL 1m [informational]: fields=high,close; distinct-days=2",
                  "examples=2026-06-03,2026-06-04",
                  "price-rows=2/252",
                  "DIAGVAL 1m [diagnostic]",
                  "current-three-source-persistent=yes",
                  "auction-candidate=yes",
                  "Cross-validation: _cross_validation.json",
                  "price-review=0 | price-informational=13",
                  "Combined flags: _combined_flags.json",
                  "ok=1 | flagged=5 | error=1 | inconclusive=1")))

        loader_bank = project / "Loader Bank"
        loader_bank.mkdir()
        duplicate_fp = seed_interval(loader_bank, "DUP")
        duplicate_entry = xval_entry(
            "DUP", duplicate_fp, [("2026-06-10", "close")])
        encoded_entry = json.dumps(
            duplicate_entry, sort_keys=True, separators=(",", ":"))
        (loader_bank / quality.XVAL_BASENAME).write_text(
            '{"DUP 1m":' + encoded_entry + ',"DUP 1m":'
            + encoded_entry + '}', encoding="utf-8")
        duplicate_xval = quality._load_cross_validation(
            loader_bank, ["DUP"])
        check("duplicate JSON sidecar key rejects the whole evidence source",
              duplicate_xval["source"] is None
              and duplicate_xval["rows"]["DUP"] == []
              and bool(duplicate_xval["errors"]))
        nan_entry = copy.deepcopy(duplicate_entry)
        nan_entry["detail"]["rows"][0]["percent"] = float("nan")
        write_json(
            loader_bank / quality.XVAL_BASENAME,
            {"DUP 1m": nan_entry}, allow_nan=True)
        nonfinite_xval = quality._load_cross_validation(
            loader_bank, ["DUP"])
        check("non-finite sidecar evidence cannot affect a verdict",
              nonfinite_xval["source"] is None
              and nonfinite_xval["rows"]["DUP"] == []
              and bool(nonfinite_xval["errors"]))

        old_bad_fp = fingerprints["BAD"]

        def mutate_bad_manifest():
            ticker_dir = bank / "BAD"
            manifest = storage.load_manifest(ticker_dir)
            manifest["fixture_race"] = True
            storage.save_manifest(ticker_dir, manifest)

        race_report = quality.build_report(
            batch(output), ["BAD"], ["BAD"], {"BAD": written("BAD")},
            storage_root=bank, run_logs_root=run_logs,
            ws7_paths=[ws7_path],
            tier0_adapter=FakeTier0(mutate=mutate_bad_manifest), now=NOW)
        raced = by_ticker(race_report)["BAD"]
        check("manifest race forces NO_CURRENT_EVIDENCE",
              raced["manifest_fingerprint"] == old_bad_fp
              and raced["quality"] == "NO_CURRENT_EVIDENCE"
              and raced["quality_source"] == "evidence_race"
              and raced["evidence_code"]
              == "evidence_changed_during_export"
              and raced["detector"] is None)
    finally:
        shutil.rmtree(project, ignore_errors=True)


if __name__ == "__main__":
    run()
    if FAILURES:
        print(f"FAILED: {len(FAILURES)}/{COUNT[0]} checks: "
              + ", ".join(FAILURES))
        raise SystemExit(1)
    print(f"ALL PASS ({COUNT[0]}/{COUNT[0]}; temp fixtures; no live export)")
