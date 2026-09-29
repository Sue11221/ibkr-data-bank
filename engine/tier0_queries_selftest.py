"""Deterministic safety and contract tests for Tier 0 queries and JSON CLI."""

from __future__ import annotations

import builtins
import contextlib
import copy
import hashlib
import inspect
import io
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import urllib.request
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import stock_storage as storage  # noqa: E402
import tier0_cli as cli  # noqa: E402
import tier0_queries as queries  # noqa: E402


FAILURES = []
COUNT = [0]


def check(name, condition, detail=""):
    COUNT[0] += 1
    print(f"[{'PASS' if condition else 'FAIL'}] {name}"
          + (f" -- {detail}" if detail and not condition else ""))
    if not condition:
        FAILURES.append(name)


def expect_error(name, code, fn):
    try:
        fn()
    except queries.Tier0Error as exc:
        check(name, exc.code == code, f"got {exc.code}: {exc}")
    except Exception as exc:  # noqa: BLE001
        check(name, False, f"wrong exception {type(exc).__name__}: {exc}")
    else:
        check(name, False, "did not raise")


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def write_json(path, payload):
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode(
        "utf-8")
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_bytes(raw)
    return raw


def artifact_record(run_id, schema, tickers, payload, run_logs, *,
                    kind, result, disposition, views, script=None,
                    superseded_by=None, resolved_by=None):
    raw = write_json(Path(run_logs) / f"{run_id}.json", payload)
    script_row = None
    if script is not None:
        script_raw = (f"# historical fixture {script}\n").encode("ascii")
        archive_basename = f"{script}.disabled"
        archive_path = (Path(run_logs).parent / "archive"
                        / "_repair_scripts_archive" / "2026-07"
                        / archive_basename)
        archive_path.parent.mkdir(parents=True, exist_ok=True)
        archive_path.write_bytes(script_raw)
        script_row = {
            "basename": script,
            "size": len(script_raw),
            "sha256": sha(script_raw),
            "archive": {
                "state": "archived_disabled",
                "basename": archive_basename,
                "archived_at": "2026-07-10T13:56:35-04:00",
            },
        }
    return {
        "run_id": run_id,
        "kind": kind,
        "tickers": tickers,
        "performed_at": "2026-07-09T12:00:00-04:00",
        "script": script_row,
        "artifact": {
            "basename": f"{run_id}.json",
            "size": len(raw),
            "sha256": sha(raw),
            "schema": schema,
        },
        "historical_result": result,
        "reviewer_state": "fixture_reviewed",
        "reviewed_disposition": disposition,
        "disposition_asof": "2026-07-10T00:00:00-04:00",
        "historical_only": True,
        "superseded_by": superseded_by,
        "resolved_by": resolved_by,
        "allowed_views": views,
    }


def write_catalog(engine, records):
    return write_json(Path(engine) / "tier0_history_catalog.json", {
        "schema_version": 1,
        "records": records,
    })


def bars(day, hour=9):
    return [
        (datetime.fromisoformat(f"{day}T{hour:02d}:30:00"),
         10.0, 11.0, 9.0, 10.5, 100),
        (datetime.fromisoformat(f"{day}T{hour:02d}:31:00"),
         10.5, 11.5, 10.0, 11.0, 200),
    ]


def seed_bank(root):
    ticker = "FTNT"
    ticker_dir = Path(root) / ticker
    ticker_dir.mkdir(parents=True, exist_ok=True)
    manifest = storage.new_manifest(ticker, ticker)
    manifest["conid"] = 101
    manifest["data_corrections"] = [{
        "ticker": ticker,
        "type": "phantom_split_correction",
        "applied": "2026-07-09T12:00:00-04:00",
        "ex_date": "2020-02-03",
        "factor": 4.0,
    }]
    for interval in ("1m", "1d"):
        months = storage.manifest_months(manifest, interval)
        for month, day in ((1, "2020-01-02"), (2, "2020-02-03")):
            stamp_hour = 0 if interval == "1d" else 9
            month_bars = bars(day, stamp_hour)
            if interval == "1d":
                month_bars = [month_bars[0]]
            path = storage.month_file_path(
                root, ticker, 2020, month, interval, fmt="csv")
            months[f"2020-{month:02d}"] = dict(
                storage.write_month_file(path, month_bars), status="present")
    storage.save_manifest(ticker_dir, manifest)


def build_fixture():
    project = Path(tempfile.mkdtemp(prefix="tier0_st_"))
    engine = project / "engine"
    bank = project / storage.STORAGE_DIR_NAME
    run_logs = project / "Run Logs"
    engine.mkdir()
    bank.mkdir()
    run_logs.mkdir()
    seed_bank(bank)
    bank_state = storage.bank_manifest_state_fingerprint(bank)

    write_json(bank / "_health_report.json", {
        "kind": "health_report", "version": 2,
        "asof": "2026-07-10T10:00:00", "clean": False,
        "counts": {"tickers": 1, "coverage_front_short": 1},
        "queue": ["FTNT"], "errors": [],
        "source_state": {
            "schema_version": storage.BANK_STATE_FINGERPRINT_VERSION,
            "algorithm": "sha256",
            "before_sha256": bank_state["sha256"],
            "after_sha256": bank_state["sha256"],
            "manifest_count": bank_state["manifest_count"],
            "current": True,
        },
    })
    write_json(bank / "_coverage_audit.json", {
        "kind": "coverage_audit", "version": 1,
        "asof": "2026-07-10T10:00:00", "ticker_count": 1,
        "bank_latest": "2020-02", "baseline_start": "2020-01",
        "forward_stale": [],
        "front_short": [{"ticker": "FTNT", "stored_first": "2020-01"}],
        "unknown": [], "errors": [],
        "summary": {"FTNT": {
            "ticker": "FTNT", "interval": "1m", "stored_first": "2020-01",
            "stored_last": "2020-02", "nmonths": 2,
            "flags": ["front-short"],
        }},
    })
    gap_series = {}
    for interval, missing_total, gap_events, days in (
            ("1m", 2, 1, 1), ("1d", 0, 0, 0)):
        fingerprint = storage.interval_state_fingerprint(bank, "FTNT", interval)
        gap_series[f"FTNT {interval}"] = {
            "missing_total": missing_total, "gap_events": gap_events,
            "days": days, "missing_days": 0, "missing_day_list": [],
            "missing_day_runs": [],
            "largest_missing_day_run": {
                "count": 0, "start": None, "end": None},
            "source_absent": 0, "source_absent_list": [],
            "ignored": 0, "ignored_list": [],
            "interval_fingerprint": {
                "schema_version": queries.gap_evidence.PROVENANCE_VERSION,
                "algorithm": "sha256",
                "before_sha256": fingerprint["sha256"],
                "after_sha256": fingerprint["sha256"],
                "current": True,
                "month_count": fingerprint["month_count"],
                "verified_absent_count": fingerprint["verified_absent_count"],
            },
        }
    write_json(bank / "_data_gaps.json", {
        "kind": queries.gap_evidence.KIND,
        "schema_version": queries.gap_evidence.SCHEMA_VERSION,
        "asof": "2026-07-10T10:00:00",
        "series": gap_series,
    })

    ftnt_old = "ticker-repair-ftnt-20260708-codex"
    ftnt_fix = "ftnt-phantom-fix-20260709-codex"
    odfl_old = "ticker-repair-odfl-backfill-20260708-codex"
    odfl_probe = "ticker-repair-odfl-availability-probe-20260708-codex"
    records = []
    records.append(artifact_record(
        ftnt_old, "ticker_repair", ["FTNT"], {
            "run": ftnt_old, "ticker": "FTNT", "result": "pass",
            "started": "2026-07-08T18:49:36-04:00",
            "finished": "2026-07-08T19:01:06-04:00",
            "boundary_day": "2014-01-13", "no_basis_action": True,
            "before": {"edges": {"1m": {"first": "2011-06"}}},
            "boundary_fill": {"result": "filled"},
            "verification": {
                "edges": {"1m": {"last": "2026-07"}},
                "traceback": "must never escape",
            },
            "gap_fill": {
                "totals": {"added": 10},
                "series": [{"ticker": "FTNT", "interval": "1m"},
                           {"ticker": "FTNT", "interval": "1d"}],
            },
            "deep_history_note": f"stored under {project}",
        }, run_logs, kind="ticker_repair", result="pass",
        disposition="superseded_by_phantom_correction",
        views=["summary", "before_after", "verification", "fetch_totals"],
        script="ticker_repair_ftnt_20260708.py",
        superseded_by=ftnt_fix))
    records.append(artifact_record(
        ftnt_fix, "phantom_fix", ["FTNT"], {
            "ticker": "FTNT", "result": "pass", "dry_run": False,
            "ex_date": "2014-01-13", "factor": 4.0,
            "affected_bars": 100, "affected_file_count": 2,
            "post_verification": {"health_clean": True},
            "manifest_note": {"type": "phantom_split_correction"},
        }, run_logs, kind="bank_correction", result="pass",
        disposition="approved_complete",
        views=["summary", "verification"]))
    records.append(artifact_record(
        odfl_old, "ticker_repair", ["ODFL"], {
            "run": odfl_old, "ticker": "ODFL", "result": "fail",
            "verification": {}, "before": {}, "gap_fill": {"totals": {}},
            "error": "did not backfill",
        }, run_logs, kind="ticker_repair", result="fail",
        disposition="resolved_source_limited",
        views=["summary", "before_after", "verification", "fetch_totals"],
        script="ticker_repair_odfl_20260708.py",
        resolved_by=odfl_probe))
    records.append(artifact_record(
        odfl_probe, "availability_probe", ["ODFL"], {
            "run": odfl_probe, "ticker": "ODFL", "result": "blocked",
            "zero_bank_writes": True, "blocker": "source limited",
            "probes": [{"kind": "daily", "rows": 0},
                       {"kind": "minute", "rows": 0}],
        }, run_logs, kind="availability_probe", result="blocked",
        disposition="source_limited_evidence",
        views=["summary", "verification"]))
    records.append(artifact_record(
        "ticker-repair-kdp-20260708-codex", "ticker_repair", ["KDP"], {
            "ticker": "KDP", "result": "pass", "verification": {},
        }, run_logs, kind="ticker_repair", result="pass",
        disposition="approved_complete",
        views=["summary", "before_after", "verification", "fetch_totals"],
        script="ticker_repair_kdp_20260708.py"))
    records.append(artifact_record(
        "ticker-repair-pcg-20260708-codex", "ticker_repair", ["PCG"], {
            "ticker": "PCG", "result": "pass", "verification": {},
        }, run_logs, kind="ticker_repair", result="pass",
        disposition="approved_complete",
        views=["summary", "before_after", "verification", "fetch_totals"],
        script="ticker_repair_pcg_20260708.py"))
    records.append(artifact_record(
        "live-seam-fetch-check-20260709-codex", "live_seam_probe",
        ["FTNT", "WBD"], {
            "task": "zero-write seam fixture", "summary": {"rows": 3},
            "rows": [
                {"ticker": "FTNT", "classification": "source_basis"},
                {"ticker": "WBD", "classification": "lineage"},
                {"ticker": "FTNT", "classification": "corrected"},
            ],
            "no_bank_writes": True, "seams_requested": 3,
        }, run_logs, kind="live_seam_probe", result="evidence_captured",
        disposition="historical_asof_probe",
        views=["summary", "seam_rows"],
        script="live_seam_fetch_check_20260709.py"))
    write_catalog(engine, records)
    return {
        "project": project, "engine": engine, "bank": bank,
        "run_logs": run_logs, "records": records,
        "ftnt_old": ftnt_old, "ftnt_fix": ftnt_fix,
        "odfl_old": odfl_old, "odfl_probe": odfl_probe,
    }


@contextlib.contextmanager
def fixed_roots(fixture):
    names = (
        "MODULE_DIR", "PROJECT_ROOT", "STORAGE_ROOT",
        "RUN_LOGS_ROOT", "SCRIPT_ARCHIVE_ROOT", "CATALOG_PATH",
    )
    old = {name: getattr(queries, name) for name in names}
    queries.MODULE_DIR = fixture["engine"]
    queries.PROJECT_ROOT = fixture["project"]
    queries.STORAGE_ROOT = fixture["bank"]
    queries.RUN_LOGS_ROOT = fixture["run_logs"]
    queries.SCRIPT_ARCHIVE_ROOT = (
        fixture["project"] / "archive" / "_repair_scripts_archive"
        / "2026-07")
    queries.CATALOG_PATH = fixture["engine"] / "tier0_history_catalog.json"
    try:
        yield
    finally:
        for name, value in old.items():
            setattr(queries, name, value)


def snapshot(root):
    out = {}
    for path in sorted(Path(root).rglob("*")):
        relative = path.relative_to(root).as_posix()
        info = path.stat()
        out[relative] = {
            "dir": path.is_dir(),
            "size": info.st_size,
            "mtime_ns": info.st_mtime_ns,
            "sha256": None if path.is_dir() else sha(path.read_bytes()),
        }
    return out


@contextlib.contextmanager
def deny_writes_network_processes():
    originals = []

    def patch(owner, name, replacement):
        originals.append((owner, name, getattr(owner, name)))
        setattr(owner, name, replacement)

    def bomb(*_args, **_kwargs):
        raise AssertionError("forbidden write/network/process API called")

    original_open = builtins.open
    original_io_open = io.open

    def guarded_open(fn):
        def wrapper(file, mode="r", *args, **kwargs):
            if any(flag in str(mode) for flag in "wax+"):
                return bomb(file, mode)
            return fn(file, mode, *args, **kwargs)
        return wrapper

    patch(builtins, "open", guarded_open(original_open))
    patch(io, "open", guarded_open(original_io_open))

    original_os_open = os.open

    def guarded_os_open(path, flags, *args, **kwargs):
        write_flags = (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC
                       | os.O_APPEND)
        if int(flags) & write_flags:
            return bomb(path, flags)
        return original_os_open(path, flags, *args, **kwargs)

    patch(os, "open", guarded_os_open)
    for name in (
            "rename", "replace", "remove", "unlink", "mkdir", "makedirs",
            "rmdir", "removedirs", "link", "symlink"):
        if hasattr(os, name):
            patch(os, name, bomb)
    for name in (
            "write_text", "write_bytes", "touch", "mkdir", "rename",
            "replace", "unlink", "rmdir", "symlink_to", "hardlink_to"):
        if hasattr(Path, name):
            patch(Path, name, bomb)
    for name in (
            "copy", "copy2", "copyfile", "copytree", "move", "rmtree"):
        patch(shutil, name, bomb)
    patch(socket, "socket", bomb)
    patch(socket, "create_connection", bomb)
    patch(urllib.request, "urlopen", bomb)
    for name in ("Popen", "run", "call", "check_call", "check_output"):
        patch(subprocess, name, bomb)
    patch(os, "system", bomb)
    try:
        yield
    finally:
        for owner, name, value in reversed(originals):
            setattr(owner, name, value)


def replace_artifact(fixture, record, payload):
    path = fixture["run_logs"] / record["artifact"]["basename"]
    raw = write_json(path, payload)
    record["artifact"]["size"] = len(raw)
    record["artifact"]["sha256"] = sha(raw)
    write_catalog(fixture["engine"], fixture["records"])
    return raw


def cli_call(argv):
    stream = io.StringIO()
    with contextlib.redirect_stdout(stream):
        code = cli.main(argv)
    text = stream.getvalue()
    try:
        payload = json.loads(text)
    except ValueError:
        payload = None
    return code, payload, text


def run_core_tests(fixture):
    check("dependency: Tier 0 does not import stock_ibkr",
          "stock_ibkr" not in sys.modules)
    for public in queries.__all__:
        if public == "Tier0Error":
            continue
        params = inspect.signature(getattr(queries, public)).parameters
        check(f"surface: {public} has no caller root/path parameter",
              not ({"root", "path", "project_root"} & set(params)), str(params))
    check("surface: no generic query/dispatch function is exported",
          not hasattr(queries, "query") and not hasattr(queries, "dispatch"))

    health = queries.bank_health_summary()
    check("health: fixed cached report is returned",
          health["counts"]["tickers"] == 1 and health["queue"] == ["FTNT"]
          and health["evidence_current"] is True)
    coverage = queries.coverage_status(ticker="FTNT")
    check("coverage: one-ticker row is bounded and current-labeled",
          coverage["status"]["stored_first"] == "2020-01")
    coverage_all = queries.coverage_status(cursor=0, limit=1)
    check("coverage: bank flags are paginated",
          coverage_all["pagination"]["total"] == 1
          and coverage_all["flagged"][0]["ticker"] == "FTNT")
    gaps = queries.cached_gap_summary(ticker="FTNT", limit=1)
    check("gaps: cached rows and totals are paginated",
          gaps["series_count"] == 2 and gaps["totals"]["missing_total"] == 2
          and gaps["pagination"]["next_cursor"] == 1
          and gaps["evidence_current"] is True
          and gaps["series"][0]["current"] is True)
    queue = queries.repair_queue(limit=1)
    check("repair queue: health queue is exposed without refresh",
          queue["queue"] == ["FTNT"] and queue["written"] is False
          and queue["evidence_current"] is True)

    original_bank_state = queries.storage.bank_manifest_state_fingerprint
    queries.storage.bank_manifest_state_fingerprint = lambda _root: {
        "schema_version": storage.BANK_STATE_FINGERPRINT_VERSION,
        "algorithm": "sha256", "sha256": "f" * 64, "manifest_count": 1,
    }
    try:
        stale_health = queries.bank_health_summary()
        stale_queue = queries.repair_queue()
    finally:
        queries.storage.bank_manifest_state_fingerprint = original_bank_state
    check("health freshness: post-report manifest state suppresses stale clean/queue",
          stale_health["evidence_current"] is False
          and stale_health["clean"] is False and stale_health["queue"] == []
          and stale_queue["evidence_current"] is False
          and stale_queue["queue"] == [],
          str((stale_health, stale_queue)))

    split = queries.split_status("FTNT")
    check("split: offline result is explicit about missing cache evidence",
          split["network"] is False and split["written"] is False
          and split["evidence_complete"] is False
          and split["generated_at"] is None)

    original_split_audit = queries.split_audit.audit

    def evidence_dated_split(*_args, **_kwargs):
        return {
            "generated_at": "2099-01-01T00:00:00+00:00",
            "tickers": [{
                "source_provenance": {
                    "fetched_at": "2026-07-09T14:00:00+00:00",
                    "reference_asof": "2026-07-09T13:00:00+00:00",
                },
                "rows": [],
            }],
        }

    queries.split_audit.audit = evidence_dated_split
    try:
        dated_split = queries.split_status("FTNT")
    finally:
        queries.split_audit.audit = original_split_audit
    check("split: generated_at comes from stable evidence, not evaluation time",
          dated_split["generated_at"] == "2026-07-09T13:00:00+00:00")
    ticker = queries.ticker_status("FTNT")
    by_interval = {row["interval"]: row for row in ticker["current"]["series"]}
    check("ticker: manifest-gated edge reads return both series",
          set(by_interval) == {"1d", "1m"}
          and by_interval["1m"]["first_month"] == "2020-01"
          and by_interval["1m"]["last_month"] == "2020-02")
    check("ticker: current and dated historical scopes are separate",
          ticker["historical"]["asof_labeled"] is True
          and ticker["current"]["coverage"]["ticker"] == "FTNT")

    history = queries.history_list(ticker="FTNT", limit=10)
    history_by_id = {row["run_id"]: row for row in history["records"]}
    old = history_by_id[fixture["ftnt_old"]]
    check("history: FTNT raw pass is labeled superseded",
          old["historical_result"] == "pass"
          and old["reviewed_disposition"] == "superseded_by_phantom_correction"
          and old["superseded_by"] == fixture["ftnt_fix"])
    check("history: script and artifact digests are verified",
          old["artifact"]["state"] == "verified"
          and old["script"]["state"] == "archived"
          and old["script"]["root_state"] == "absent"
          and old["script"]["archive"]["integrity_state"] == "verified"
          and old["script"]["archive"]["relative_path"].startswith(
              "archive/_repair_scripts_archive/2026-07/"))
    odfl = queries.history_list(ticker="ODFL", limit=10)
    odfl_by_id = {row["run_id"]: row for row in odfl["records"]}
    old_odfl = odfl_by_id[fixture["odfl_old"]]
    check("history: ODFL raw fail is labeled resolved/source-limited",
          old_odfl["historical_result"] == "fail"
          and old_odfl["reviewed_disposition"] == "resolved_source_limited"
          and old_odfl["resolved_by"] == fixture["odfl_probe"])

    result = queries.run_result(fixture["ftnt_old"])
    check("run result: verified summary remains historical, never current",
          result["current"] is None and result["historical"] is True
          and result["record"]["artifact"]["state"] == "verified")
    check("run result: absolute project paths are normalized",
          str(fixture["project"]) not in result["payload"]["deep_history_note"])
    verification = queries.run_result(
        fixture["ftnt_old"], view="verification")
    check("run result: tracebacks are omitted from semantic views",
          "traceback" not in verification["payload"])
    fetch = queries.run_result(
        fixture["ftnt_old"], view="fetch_totals", limit=1)
    check("run result: fetch series is paginated",
          fetch["pagination"]["total"] == 2
          and fetch["pagination"]["next_cursor"] == 1)
    seam = queries.run_result(
        "live-seam-fetch-check-20260709-codex",
        view="seam_rows", cursor=1, limit=1)
    check("run result: seam evidence uses bounded rows",
          seam["pagination"]["returned"] == 1
          and seam["payload"]["rows"][0]["ticker"] == "WBD")
    probe = queries.run_result(
        fixture["odfl_probe"], view="verification", limit=1)
    check("run result: availability probes are paginated",
          probe["pagination"]["total"] == 2)
    fix = queries.run_result(fixture["ftnt_fix"], view="verification")
    check("run result: successor correction is first-class evidence",
          fix["payload"]["post_verification"]["health_clean"] is True)

    expect_error("validation: lowercase ticker is rejected", "invalid_request",
                 lambda: queries.ticker_status("ftnt"))
    expect_error("validation: traversal-like ticker is rejected", "invalid_request",
                 lambda: queries.ticker_status("../FTNT"))
    expect_error("validation: traversal-like run id is rejected", "invalid_request",
                 lambda: queries.run_result("../run"))
    expect_error("validation: unsupported semantic view is rejected", "invalid_request",
                 lambda: queries.run_result(fixture["ftnt_old"], view="raw"))
    expect_error("validation: excessive page limit is rejected", "invalid_request",
                 lambda: queries.history_list(limit=101))
    expect_error("lookup: unknown run id is deterministic", "not_found",
                 lambda: queries.run_result("missing-run"))


def run_streamed_gap_tests(fixture):
    original_chunk_bytes = queries.STREAM_CHUNK_BYTES
    queries.STREAM_CHUNK_BYTES = 1
    try:
        number_stream = queries._JsonStream(
            io.BytesIO(b"123,"), basename="number.json", max_bytes=4)
        check("gap stream: split numeric token waits for its delimiter",
              number_stream.value(max_chars=3) == 123
              and number_stream.consume(","))
        unicode_stream = queries._JsonStream(
            io.BytesIO('"café",'.encode("utf-8")),
            basename="unicode.json", max_bytes=8)
        check("gap stream: split multibyte UTF-8 token decodes incrementally",
              unicode_stream.value(max_chars=6) == "café"
              and unicode_stream.consume(","))
    finally:
        queries.STREAM_CHUNK_BYTES = original_chunk_bytes

    gap_path = fixture["bank"] / queries.GAP_REPORT
    original = gap_path.read_bytes()
    payload = json.loads(original.decode("utf-8"))
    for index in range(2100):
        payload["series"][f"ZX{index:04d} 1m"] = {
            "padding": "x" * 1000,
        }
    raw = write_json(gap_path, payload)
    try:
        check("gap stream: fixture crosses ordinary sidecar cap only",
              queries.MAX_SIDECAR_BYTES < len(raw)
              < queries.gap_evidence.MAX_BYTES,
              str(len(raw)))
        check("gap stream: ordinary sidecar cap remains exactly 2 MiB",
              queries.MAX_SIDECAR_BYTES == 2 * 1024 * 1024)
        expect_error(
            "gap stream: old full-file loader is RED on the fixture",
            "evidence_too_large",
            lambda: queries._load_sidecar(queries.GAP_REPORT))

        filtered, _evidence = queries._stream_gap_sidecar(ticker="FTNT")
        check("gap stream: direct ticker selector retains only matching keys",
              set(filtered["series"]) == {"FTNT 1d", "FTNT 1m"},
              str(sorted(filtered["series"])))

        original_state = queries._file_state
        calls = [0]

        def direct_gap_raced(path_arg, **kwargs):
            state = original_state(path_arg, **kwargs)
            if Path(path_arg).name == queries.GAP_REPORT:
                calls[0] += 1
                if calls[0] == 2:
                    state = dict(state)
                    state["mtime_ns"] += 1
            return state

        queries._file_state = direct_gap_raced
        try:
            expect_error(
                "gap stream: direct post-read identity race fails closed",
                "evidence_changed",
                lambda: queries._stream_gap_sidecar(ticker="FTNT"))
        finally:
            queries._file_state = original_state

        ticker = queries.ticker_status("FTNT")
        ticker_gaps = {
            row["interval"]: row
            for row in ticker["current"]["cached_gaps"]
        }
        check("gap stream: ticker status succeeds above 2 MiB",
              set(ticker_gaps) == {"1d", "1m"}
              and ticker_gaps["1m"]["missing_total"] == 2
              and any(Path(path).name == queries.GAP_REPORT
                      for path in ticker["evidence"]["paths"]),
              str((ticker_gaps, ticker.get("evidence"))))

        gaps = queries.cached_gap_summary(ticker="FTNT", limit=1)
        check("gap stream: cached gaps succeeds above 2 MiB with pagination",
              gaps["series_count"] == 2
              and gaps["totals"]["missing_total"] == 2
              and gaps["pagination"]["returned"] == 1
              and gaps["pagination"]["next_cursor"] == 1
              and gaps["evidence_current"] is True,
              str(gaps))
    finally:
        gap_path.write_bytes(original)

    minimal = {
        "kind": queries.gap_evidence.KIND,
        "schema_version": queries.gap_evidence.SCHEMA_VERSION,
        "asof": "2026-07-10T10:00:00",
        "series": {
            f"ZX{index:04d} 1m": {}
            for index in range(queries.gap_evidence.MAX_SERIES + 1)
        },
    }
    try:
        write_json(gap_path, minimal)
        expect_error(
            "gap stream: 4,001 series exceed the source-schema bound",
            "invalid_evidence",
            lambda: queries.cached_gap_summary(ticker="FTNT"))

        gap_path.write_bytes(original + b"x")
        expect_error(
            "gap stream: trailing garbage after the root object fails closed",
            "invalid_evidence",
            lambda: queries.cached_gap_summary(ticker="FTNT"))
    finally:
        gap_path.write_bytes(original)


def run_integrity_tests(fixture):
    record = next(row for row in fixture["records"]
                  if row["run_id"] == fixture["ftnt_old"])
    path = fixture["run_logs"] / record["artifact"]["basename"]
    original = path.read_bytes()
    path.unlink()
    listed = queries.history_list(ticker="FTNT", limit=10)
    state = {row["run_id"]: row for row in listed["records"]}[
        fixture["ftnt_old"]]["artifact"]["state"]
    check("integrity: missing artifact is visible without exposing details",
          state == "missing")
    expect_error("integrity: missing artifact details fail closed",
                 "evidence_missing",
                 lambda: queries.run_result(fixture["ftnt_old"]))
    path.write_bytes(original)

    script = record["script"]
    archive_path = (fixture["project"] / "archive"
                    / "_repair_scripts_archive" / "2026-07"
                    / script["archive"]["basename"])
    archive_raw = archive_path.read_bytes()
    archive_path.unlink()
    listed = queries.history_list(ticker="FTNT", limit=10)
    script_summary = {row["run_id"]: row for row in listed["records"]}[
        fixture["ftnt_old"]]["script"]
    check("archive integrity: missing disabled bytes are explicit",
          script_summary["state"] == "archive_missing"
          and script_summary["root_state"] == "absent",
          str(script_summary))
    archive_path.write_bytes(archive_raw)

    changed_script = bytearray(archive_raw)
    changed_script[-1] ^= 1
    archive_path.write_bytes(bytes(changed_script))
    listed = queries.history_list(ticker="FTNT", limit=10)
    script_summary = {row["run_id"]: row for row in listed["records"]}[
        fixture["ftnt_old"]]["script"]
    check("archive integrity: wrong disabled digest is explicit",
          script_summary["state"] == "archive_integrity_error",
          str(script_summary))
    archive_path.write_bytes(archive_raw)

    root_script = fixture["project"] / script["basename"]
    root_script.write_bytes(archive_raw)
    listed = queries.history_list(ticker="FTNT", limit=10)
    script_summary = {row["run_id"]: row for row in listed["records"]}[
        fixture["ftnt_old"]]["script"]
    check("archive integrity: restored root executor is loud residue",
          script_summary["state"] == "archived_root_residue"
          and script_summary["root_state"] == "verified",
          str(script_summary))
    root_script.unlink()

    path.write_bytes(b"x" * (queries.MAX_ARTIFACT_BYTES + 1))
    expect_error("bounds: actual artifact over 2 MiB is refused before parse",
                 "evidence_too_large",
                 lambda: queries.run_result(fixture["ftnt_old"]))
    path.write_bytes(original)

    changed = bytearray(original)
    changed[-1] = changed[-1] ^ 1
    path.write_bytes(bytes(changed))
    expect_error("integrity: wrong artifact SHA fails before parse",
                 "artifact_integrity",
                 lambda: queries.run_result(fixture["ftnt_old"]))
    path.write_bytes(original)

    original_payload = json.loads(original.decode("utf-8"))
    replace_artifact(fixture, record, {"ticker": "FTNT", "result": "pass"})
    expect_error("schema: matching-hash malformed artifact is rejected",
                 "invalid_artifact_schema",
                 lambda: queries.run_result(fixture["ftnt_old"]))
    replace_artifact(fixture, record, original_payload)

    wrong_ticker = copy.deepcopy(original_payload)
    wrong_ticker["ticker"] = "KDP"
    replace_artifact(fixture, record, wrong_ticker)
    expect_error("schema: artifact ticker/catalog mismatch is rejected",
                 "invalid_artifact_schema",
                 lambda: queries.run_result(fixture["ftnt_old"]))
    replace_artifact(fixture, record, original_payload)

    seam_record = next(row for row in fixture["records"]
                       if row["artifact"]["schema"] == "live_seam_probe")
    seam_path = fixture["run_logs"] / seam_record["artifact"]["basename"]
    seam_payload = json.loads(seam_path.read_text(encoding="utf-8"))
    wrong_seam = copy.deepcopy(seam_payload)
    wrong_seam["rows"] = [
        row for row in wrong_seam["rows"] if row.get("ticker") != "WBD"]
    replace_artifact(fixture, seam_record, wrong_seam)
    expect_error("schema: seam ticker set/catalog mismatch is rejected",
                 "invalid_artifact_schema",
                 lambda: queries.run_result(
                     seam_record["run_id"], view="summary"))
    replace_artifact(fixture, seam_record, seam_payload)

    bad_catalog = copy.deepcopy(fixture["records"])
    bad_catalog[0]["artifact"]["basename"] = "../escape.json"
    write_catalog(fixture["engine"], bad_catalog)
    expect_error("catalog: traversal basename is rejected", "invalid_catalog",
                 lambda: queries.history_list())
    write_catalog(fixture["engine"], fixture["records"])

    bad_catalog = copy.deepcopy(fixture["records"])
    archived_record = next(row for row in bad_catalog if row.get("script"))
    archived_record["script"]["archive"]["basename"] = "other.py.disabled"
    write_catalog(fixture["engine"], bad_catalog)
    expect_error("catalog: archive basename must derive from script",
                 "invalid_catalog", lambda: queries.history_list())
    write_catalog(fixture["engine"], fixture["records"])

    bad_catalog = copy.deepcopy(fixture["records"])
    bad_catalog[0]["artifact"]["size"] = queries.MAX_ARTIFACT_BYTES + 1
    write_catalog(fixture["engine"], bad_catalog)
    expect_error("catalog: oversized artifact declaration is rejected",
                 "invalid_catalog", lambda: queries.history_list())
    write_catalog(fixture["engine"], fixture["records"])

    bad_catalog = copy.deepcopy(fixture["records"])
    bad_catalog[0]["superseded_by"] = "missing-successor"
    write_catalog(fixture["engine"], bad_catalog)
    expect_error("catalog: dangling successor is rejected", "invalid_catalog",
                 lambda: queries.history_list())
    write_catalog(fixture["engine"], fixture["records"])

    bad_catalog = copy.deepcopy(fixture["records"])
    bad_catalog[0]["performed_at"] = "2026-07-10"
    write_catalog(fixture["engine"], bad_catalog)
    expect_error("catalog: timezone-free timestamp is rejected",
                 "invalid_catalog", lambda: queries.history_list())
    write_catalog(fixture["engine"], fixture["records"])

    original_reparse = queries._is_reparse
    queries._is_reparse = lambda _state: True
    try:
        expect_error("path: reparse evidence is refused", "unsafe_path",
                     lambda: queries.bank_health_summary())
        expect_error("path: reparse streamed gap evidence is refused",
                     "unsafe_path",
                     lambda: queries.cached_gap_summary(ticker="FTNT"))
    finally:
        queries._is_reparse = original_reparse

    original_state = queries._file_state
    calls = [0]

    def raced(path_arg, **kwargs):
        state = original_state(path_arg, **kwargs)
        if Path(path_arg).name == queries.HEALTH_REPORT:
            calls[0] += 1
            if calls[0] == 2:
                state = dict(state)
                state["mtime_ns"] += 1
        return state

    queries._file_state = raced
    try:
        expect_error("race: changing sidecar fails with no partial payload",
                     "evidence_changed", lambda: queries.bank_health_summary())
    finally:
        queries._file_state = original_state

    calls = [0]

    def artifact_raced(path_arg, **kwargs):
        state = original_state(path_arg, **kwargs)
        if Path(path_arg).name == record["artifact"]["basename"]:
            calls[0] += 1
            if calls[0] == 2:
                state = dict(state)
                state["ctime_ns"] += 1
        return state

    queries._file_state = artifact_raced
    try:
        expect_error("race: changing artifact fails before any payload",
                     "evidence_changed",
                     lambda: queries.run_result(fixture["ftnt_old"]))
    finally:
        queries._file_state = original_state

    calls = [0]

    def manifest_raced(path_arg, **kwargs):
        state = original_state(path_arg, **kwargs)
        if Path(path_arg).name == storage.MANIFEST_NAME:
            calls[0] += 1
            if calls[0] == 2:
                state = dict(state)
                state["mtime_ns"] += 1
        return state

    queries._file_state = manifest_raced
    try:
        expect_error("race: changing manifest fails before ticker payload",
                     "evidence_changed", lambda: queries.ticker_status("FTNT"))
    finally:
        queries._file_state = original_state

    gap_path = fixture["bank"] / queries.GAP_REPORT
    gap_raw = gap_path.read_bytes()
    bad_gap = json.loads(gap_raw.decode("utf-8"))
    bad_gap["series"]["FTNT 1m"]["missing_total"] = "two"
    write_json(gap_path, bad_gap)
    bad_gap_result = queries.cached_gap_summary(ticker="FTNT")
    check("schema: corrupt cached numeric count is explicit and non-current",
          bad_gap_result["evidence_current"] is False
          and any(row.get("state") == "malformed"
                  for row in bad_gap_result["unavailable"]),
          str(bad_gap_result))
    gap_path.write_bytes(gap_raw)

    gap_path.write_bytes(b'{"asof":')
    expect_error("gap stream: malformed JSON fails closed",
                 "invalid_evidence",
                 lambda: queries.cached_gap_summary(ticker="FTNT"))
    gap_path.write_bytes(gap_raw)

    gap_path.write_bytes(
        b'{"asof":"2026-07-10T10:00:00","kind":"data_gaps",'
        b'"schema_version":1,"series":{},"series":{}}')
    expect_error("gap stream: duplicate object key fails closed",
                 "invalid_evidence",
                 lambda: queries.cached_gap_summary(ticker="FTNT"))
    gap_path.write_bytes(gap_raw)

    gap_path.write_bytes(b" " * (queries.gap_evidence.MAX_BYTES + 1))
    expect_error("gap stream: source-schema byte cap remains fail-closed",
                 "evidence_too_large",
                 lambda: queries.cached_gap_summary(ticker="FTNT"))
    gap_path.write_bytes(gap_raw)

    calls = [0]

    def gap_raced(path_arg, **kwargs):
        state = original_state(path_arg, **kwargs)
        if Path(path_arg).name == queries.GAP_REPORT:
            calls[0] += 1
            if calls[0] == 2:
                state = dict(state)
                state["mtime_ns"] += 1
        return state

    queries._file_state = gap_raced
    try:
        expect_error("race: changing streamed gap evidence fails closed",
                     "evidence_changed",
                     lambda: queries.cached_gap_summary(ticker="FTNT"))
    finally:
        queries._file_state = original_state

    probe_record = next(row for row in fixture["records"]
                        if row["run_id"] == fixture["odfl_probe"])
    probe_path = fixture["run_logs"] / probe_record["artifact"]["basename"]
    probe_payload = json.loads(probe_path.read_text(encoding="utf-8"))
    long_probe = dict(probe_payload)
    long_probe["blocker"] = "b" * (queries.MAX_STRING + 500)
    replace_artifact(fixture, probe_record, long_probe)
    capped = queries.run_result(fixture["odfl_probe"], view="summary")
    check("bounds: artifact strings are capped at 1,000 characters",
          len(capped["payload"]["blocker"]) == queries.MAX_STRING)
    replace_artifact(fixture, probe_record, probe_payload)

    huge_payload = copy.deepcopy(original_payload)
    huge_payload["verification"] = {
        "a": ["x" * 1000 for _ in range(100)],
        "b": ["y" * 1000 for _ in range(100)],
        "c": ["z" * 1000 for _ in range(100)],
    }
    replace_artifact(fixture, record, huge_payload)
    expect_error("bounds: encoded output over 256 KiB is refused",
                 "output_too_large",
                 lambda: queries.run_result(
                     fixture["ftnt_old"], view="verification"))
    replace_artifact(fixture, record, original_payload)


def run_gap_kind_policy_tests():
    base = Path(tempfile.mkdtemp(prefix="tier0_gap_kind_"))
    bank = base / storage.STORAGE_DIR_NAME
    ticker_dir = bank / "FTNT"
    ticker_dir.mkdir(parents=True)
    old_root = queries.STORAGE_ROOT
    try:
        interval = "1m-iv"
        month_rows = [
            (datetime.fromisoformat("2020-01-02T09:30:00"),
             0.20, 0.21, 0.19, 0.205, 0),
            (datetime.fromisoformat("2020-01-02T09:31:00"),
             0.205, 0.22, 0.20, 0.215, 0),
        ]
        stats = storage.write_month_file(
            storage.month_file_path(
                bank, "FTNT", 2020, 1, interval, fmt="csv"),
            month_rows)
        manifest = storage.new_manifest("FTNT", "FTNT")
        storage.manifest_months(manifest, interval)["2020-01"] = dict(
            stats, status="present")
        storage.save_manifest(ticker_dir, manifest)
        fingerprint = storage.interval_state_fingerprint(
            bank, "FTNT", interval)
        write_json(bank / queries.GAP_REPORT, {
            "kind": queries.gap_evidence.KIND,
            "schema_version": queries.gap_evidence.SCHEMA_VERSION,
            "asof": "2026-07-21T12:00:00-04:00",
            "series": {f"FTNT {interval}": {
                "missing_total": 0, "gap_events": 0, "days": 0,
                "missing_days": 0, "missing_day_list": [],
                "missing_day_runs": [],
                "largest_missing_day_run": {
                    "count": 0, "start": None, "end": None},
                "source_absent": 0, "source_absent_list": [],
                "ignored": 0, "ignored_list": [],
                "interval_fingerprint": {
                    "schema_version":
                        queries.gap_evidence.PROVENANCE_VERSION,
                    "algorithm": "sha256",
                    "before_sha256": fingerprint["sha256"],
                    "after_sha256": fingerprint["sha256"],
                    "current": True,
                    "month_count": fingerprint["month_count"],
                    "verified_absent_count":
                        fingerprint["verified_absent_count"],
                },
            }},
        })
        queries.STORAGE_ROOT = bank
        explicit_kind = queries.cached_gap_summary(
            ticker="FTNT", interval=interval)
        check("gaps: explicit RTH kind is applicable and requests one series",
              explicit_kind["applicable"] is True
              and explicit_kind["expected_series_count"] == 1
              and explicit_kind["series_count"] == 1
              and explicit_kind["evidence_current"] is True
              and explicit_kind["series"][0]["interval"] == interval,
              str(explicit_kind))

        kind_pre = queries.cached_gap_summary(
            ticker="FTNT", interval="1m-iv-pre")
        check("gaps: explicit kind-pre is inapplicable and requests no series",
              kind_pre["applicable"] is False
              and kind_pre["expected_series_count"] == 0
              and kind_pre["series_count"] == 0
              and kind_pre["unavailable"] == [],
              str(kind_pre))
    finally:
        queries.STORAGE_ROOT = old_root
        shutil.rmtree(base, ignore_errors=True)


def run_safety_tests(fixture):
    before = snapshot(fixture["bank"])
    with deny_writes_network_processes():
        results = [
            queries.bank_health_summary(),
            queries.coverage_status(ticker="FTNT"),
            queries.cached_gap_summary(ticker="FTNT"),
            queries.repair_queue(),
            queries.history_list(ticker="FTNT"),
            queries.run_result(fixture["ftnt_old"]),
            queries.split_status("FTNT"),
            queries.ticker_status("FTNT"),
        ]
    check("safety: all Tier 0 operations pass write/network/process traps",
          all(result.get("written") is False
              or result.get("historical") is True
              or result.get("kind") == "tier0_history_list"
              for result in results))
    check("safety: complete fixture bank bytes and metadata are unchanged",
          snapshot(fixture["bank"]) == before)


def run_cli_tests(fixture):
    commands = [
        ["bank-health"],
        ["ticker-status", "--ticker", "FTNT"],
        ["coverage-status", "--ticker", "FTNT"],
        ["split-status", "--ticker", "FTNT"],
        ["cached-gaps", "--ticker", "FTNT"],
        ["repair-queue"],
        ["history-list", "--ticker", "FTNT"],
        ["run-result", "--run-id", fixture["ftnt_old"]],
    ]
    for args in commands:
        code, payload, text = cli_call(args)
        check(f"CLI: {' '.join(args)} returns one JSON success envelope",
              code == 0 and payload is not None and payload.get("ok") is True
              and "Traceback" not in text and text.count("\n") == 1,
              text[:300])
    code, payload, text = cli_call([
        "ticker-status", "--ticker", "ftnt"])
    check("CLI: validation error is JSON with deterministic exit 2",
          code == 2 and payload["ok"] is False
          and payload["error"]["code"] == "invalid_request"
          and "Traceback" not in text, text)
    code, payload, text = cli_call(["bank-health", "--root", "."])
    check("CLI: caller-selected root is rejected as JSON",
          code == 2 and payload["error"]["code"] == "invalid_arguments"
          and "Traceback" not in text, text)
    code, payload, text = cli_call(["--help"])
    check("CLI: root help is a bounded JSON envelope",
          code == 0 and payload["ok"] is True
          and payload["data"]["kind"] == "tier0_cli_help"
          and "ticker-status" in payload["data"]["commands"], text)
    code, payload, text = cli_call(["ticker-status", "--help"])
    check("CLI: command help is a bounded JSON envelope",
          code == 0 and list(payload["data"]["commands"]) == ["ticker-status"],
          text)
    original = queries.bank_health_summary
    queries.bank_health_summary = lambda: (_ for _ in ()).throw(
        RuntimeError("secret details"))
    try:
        code, payload, text = cli_call(["bank-health"])
    finally:
        queries.bank_health_summary = original
    check("CLI: unexpected failures never emit traceback/details",
          code == 4 and payload["error"]["code"] == "internal_error"
          and "secret details" not in text and "Traceback" not in text, text)


def main():
    fixture = build_fixture()
    try:
        with fixed_roots(fixture):
            run_core_tests(fixture)
            run_streamed_gap_tests(fixture)
            run_gap_kind_policy_tests()
            run_integrity_tests(fixture)
            run_safety_tests(fixture)
            run_cli_tests(fixture)
    finally:
        shutil.rmtree(fixture["project"], ignore_errors=True)
    print()
    if FAILURES:
        print(f"{COUNT[0]} checks, {len(FAILURES)} failed")
        print(f"{len(FAILURES)} FAILURE(S): {FAILURES}")
        return 1
    print(f"{COUNT[0]} checks, 0 failed")
    print(f"TIER 0 SELF-TESTS PASSED: {COUNT[0]}/{COUNT[0]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
