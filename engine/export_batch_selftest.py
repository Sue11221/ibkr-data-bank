"""Adversarial and byte-identity tests for the shared batch orchestrator."""

from __future__ import annotations

import ast
import contextlib
import datetime as dt
import hashlib
import json
import queue
import shutil
import sys
import tempfile
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import export_batch as batch  # noqa: E402
import export_csv  # noqa: E402
import export_designer  # noqa: E402
import export_quality  # noqa: E402
import stock_storage as storage  # noqa: E402
import stock_validate  # noqa: E402
import vol_value_audit  # noqa: E402


FAILURES = []
COUNT = [0]
NOW = dt.datetime(
    2026, 7, 10, 18, 30, 15,
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
    except batch.BatchExportError:
        check(name, True)
    except Exception as exc:  # noqa: BLE001
        check(name, False, f"wrong exception {type(exc).__name__}: {exc}")
    else:
        check(name, False, "did not raise")


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def seed_ticker(root, ticker, offset=0.0):
    ticker_dir = Path(root) / ticker
    ticker_dir.mkdir(parents=True)
    manifest = storage.new_manifest(ticker, ticker)
    manifest["conid"] = 10_000 + sum(ord(char) for char in ticker)
    bars = [
        (dt.datetime(2026, 1, 2, 9, 30), 10.0 + offset, 11.0 + offset,
         9.5 + offset, 10.5 + offset, 100),
        (dt.datetime(2026, 1, 2, 9, 31), 10.5 + offset, 11.5 + offset,
         10.0 + offset, 11.0 + offset, 200),
        (dt.datetime(2026, 1, 5, 9, 30), 11.0 + offset, 12.0 + offset,
         10.5 + offset, 11.5 + offset, 300),
    ]
    path = storage.month_file_path(
        root, ticker, 2026, 1, "1m", fmt="csv")
    storage.manifest_months(manifest, "1m")["2026-01"] = \
        storage.write_month_file(path, bars)
    storage.save_manifest(ticker_dir, manifest)
    return sha((ticker_dir / storage.MANIFEST_NAME).read_bytes())


def seed_ratio(root, ticker, stamps, value=0.3):
    ticker_dir = Path(root) / ticker
    manifest = storage.load_manifest(ticker_dir)
    bars = [
        (stamp, value, value, value, value, 0)
        for stamp in stamps
    ]
    path = storage.month_file_path(
        root, ticker, 2026, 1, "1m-iv", fmt="csv")
    storage.manifest_months(manifest, "1m-iv")["2026-01"] = (
        storage.write_month_file(path, bars))
    storage.save_manifest(ticker_dir, manifest)


def write_ws7(path, fingerprints):
    verdicts = export_quality.WS7_VERDICTS
    rows = [
        {"ticker": ticker, "manifest_fingerprint": fingerprint,
         "verdict": "CLEAN"}
        for ticker, fingerprint in sorted(fingerprints.items())
    ]
    counts = {verdict: 0 for verdict in verdicts}
    counts["CLEAN"] = len(rows)
    payload = {
        "kind": export_quality.WS7_KIND,
        "version": export_quality.WS7_VERSION,
        "report_only": True,
        "provider": "stockanalysis",
        "reference_range": "Max",
        "finished_at": "2026-07-10T22:00:00+00:00",
        "params": {"ticker_count": len(rows)},
        "counts": counts,
        "rows": rows,
    }
    raw = json.dumps(
        payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    Path(path).write_bytes(raw)
    return raw


class FakeTier0:
    def repair_queue(self, *, cursor=0, limit=100):
        return {
            "kind": "tier0_repair_queue",
            "asof": "2026-07-10T18:00:00-04:00",
            "evidence_current": True,
            "queue": [],
            "pagination": {
                "cursor": cursor, "limit": limit, "returned": 0,
                "next_cursor": None,
            },
            "network": False,
            "written": False,
            "evidence": {
                "fingerprint": "a" * 64,
                "paths": ["fixture-health.json"],
            },
        }

    def cached_gap_summary(self, *, ticker=None, interval=None,
                           cursor=0, limit=10):
        return {
            "kind": "tier0_cached_gap_summary",
            "asof": "2026-07-13T17:00:00-04:00",
            "filters": {"ticker": ticker, "interval": interval},
            "applicable": True,
            "evidence_current": True,
            "series": [{
                "ticker": ticker, "interval": interval, "current": True,
                "missing_total": 0, "gap_events": 0, "days": 0,
                "missing_days": 0, "missing_day_list": [],
                "missing_day_runs": [],
                "largest_missing_day_run": {
                    "count": 0, "start": None, "end": None},
                "source_absent": 0, "source_absent_list": [],
                "ignored": 0, "ignored_list": [],
                "asof": "2026-07-13T17:00:00-04:00",
                "interval_fingerprint": {
                    "current": True, "before_sha256": "c" * 64,
                    "after_sha256": "c" * 64,
                },
            }],
            "unavailable": [], "errors": [],
            "pagination": {
                "cursor": cursor, "limit": limit, "returned": 1,
                "next_cursor": None,
            },
            "network": False, "written": False,
            "evidence": {
                "fingerprint": "c" * 64,
                "paths": ["fixture-gaps.json"],
            },
        }

    def ticker_status(self, ticker):
        raise AssertionError(f"unexpected ticker detail query: {ticker}")


def quality_options(run_logs, ws7_path):
    return {
        "run_logs_root": run_logs,
        "ws7_paths": [ws7_path],
        "tier0_adapter": FakeTier0(),
    }


def tree_snapshot(root):
    out = []
    for path in sorted(item for item in Path(root).rglob("*") if item.is_file()):
        raw = path.read_bytes()
        stat = path.stat()
        out.append((
            path.relative_to(root).as_posix(), len(raw), stat.st_mtime_ns,
            sha(raw)))
    return out


@contextlib.contextmanager
def aggregate_rate(seconds):
    previous = batch.AGGREGATE_EMIT_MIN_S
    batch.AGGREGATE_EMIT_MIN_S = seconds
    try:
        yield
    finally:
        batch.AGGREGATE_EMIT_MIN_S = previous


class _FakeVar:
    def __init__(self, value=""):
        self.value = value
        self.history = [value]

    def set(self, value):
        self.value = value
        self.history.append(value)

    def get(self):
        return self.value


class _FakeWidget:
    def __init__(self, kind, parent=None, **options):
        self.kind = kind
        self.parent = parent
        self.options = dict(options)
        self.config_history = []
        self.pack_history = []

    def configure(self, **options):
        self.options.update(options)
        self.config_history.append(dict(options))

    config = configure

    def pack(self, **options):
        self.pack_history.append(dict(options))

    @staticmethod
    def winfo_exists():
        return True


class _FakeTtk:
    def __init__(self):
        self.created = []

    def _new(self, kind, parent=None, **options):
        widget = _FakeWidget(kind, parent, **options)
        self.created.append(widget)
        return widget

    def Frame(self, parent=None, **options):
        return self._new("Frame", parent, **options)

    def Progressbar(self, parent=None, **options):
        return self._new("Progressbar", parent, **options)

    def Label(self, parent=None, **options):
        return self._new("Label", parent, **options)


class _FakeTk:
    TOP = "top"
    X = "x"
    LEFT = "left"
    RIGHT = "right"
    DISABLED = "disabled"


def _load_export_gui_bits(fake_ttk):
    """AST-load the tiny GUI seam without importing bootstrap-heavy Tk code."""
    source_path = Path(__file__).resolve().parents[1] / "display_data.py"
    tree = ast.parse(source_path.read_text(encoding="utf-8"))
    app = next(node for node in tree.body
               if isinstance(node, ast.ClassDef)
               and node.name == "DataViewerApp")
    names = {
        "_fmt_dur", "_export_progress_surface", "_export_progress_reset",
        "_export_progress_apply", "_export_progress_freeze",
        "_export_progress_finish", "_export_progress_pulse_start",
        "_export_progress_pulse_cancel", "_export_progress_pulse",
        "_exd_cancel_export",
        "_exd_export_poll", "_storage_export_poll", "_exd_bank_inventory",
    }
    methods = [node for node in app.body
               if isinstance(node, ast.FunctionDef) and node.name in names]
    coalescer = next(node for node in tree.body
                     if isinstance(node, ast.ClassDef)
                     and node.name == "_ExportProgressCoalescer")
    namespace = {
        "threading": threading,
        "time": time,
        "export_batch": batch,
        "stock_storage": storage,
        "stock_validate": stock_validate,
        "ttk": fake_ttk,
        "tk": _FakeTk,
    }
    module = ast.Module(body=[coalescer, *methods], type_ignores=[])
    ast.fix_missing_locations(module)
    exec(compile(module, str(source_path), "exec"), namespace)
    return namespace, app


def _fake_export_ui(namespace):
    attrs = {
        "_BATCH_RATE_WINDOW_S": 900.0,
        "_BATCH_MIN_SAMPLES": 5,
        "_EXPORT_PULSE_MS": 400,
    }
    for name in (
            "_fmt_dur", "_export_progress_surface",
            "_export_progress_reset", "_export_progress_apply",
            "_export_progress_freeze", "_export_progress_finish",
            "_export_progress_pulse_start", "_export_progress_pulse_cancel",
            "_export_progress_pulse",
            "_exd_cancel_export", "_exd_export_poll",
            "_storage_export_poll", "_exd_bank_inventory"):
        attrs[name] = namespace[name]
    attrs["_EXD_INTERVAL_ORDER"] = [
        "1s", "5s", "10s", "15s", "30s", "1m", "2m", "3m", "5m",
        "10m", "15m", "30m", "1h", "2h", "4h", "1d",
    ]
    return type("FakeExportUi", (), attrs)()


def run():
    project = Path(tempfile.mkdtemp(prefix="export-batch-selftest-"))
    bank = project / storage.STORAGE_DIR_NAME
    logs = project / "Run Logs"
    output = project / "Output"
    for path in (bank, logs, output):
        path.mkdir(parents=True)
    fingerprints = {
        "AAA": seed_ticker(bank, "AAA", 0.0),
        "BBB": seed_ticker(bank, "BBB", 20.0),
        "CCC": seed_ticker(bank, "CCC", 40.0),
        "DDD": seed_ticker(bank, "DDD", 60.0),
    }
    ws7_path = logs / "external-sweep-fixture-v2.json"
    write_ws7(ws7_path, fingerprints)
    quality = quality_options(logs, ws7_path)

    try:
        events = []

        def mixed_exporter(ticker, target, progress, _cancel):
            progress("fixture progress")
            if ticker == "AAA":
                target.write_bytes(b"AAA-DATA\n")
                return {"rows": 3, "holes": [
                    "rth:2026-02", "pre:not-archived"]}
            return {"rows": 0, "holes": []}

        result = batch.run_batch(
            bank, ["AAA", "BBB", "MISS"], ["AAA", "BBB"], output,
            interval="1m", file_format="csv", sessions="rth+pre",
            start="preset:furthest", end="per-ticker-latest",
            filename_for=lambda ticker: f"{ticker}.csv",
            exporter=mixed_exporter, workers=2, progress=events.append,
            now=NOW, quality_kwargs=quality)
        folder = Path(result["bundle_folder"])
        data_folder = Path(result["folder"])
        check("multi batch creates the exact Row 20 folder",
              folder.name ==
              "Export 2026-07-10 183015 - 3 tickers - 1m - csv"
              and result["folder_created"] is True)
        check("result preserves every selected ticker in original order",
              [row["ticker"] for row in result["results"]]
              == ["AAA", "BBB", "MISS"])
        check("mixed batch state and summary are honest",
              result["state"] == "PARTIAL"
              and result["summary"] == {
                  "files_written": 1, "rows": 3, "empty": 1,
                  "failed": 0, "cancelled": 0, "not_in_bank": 1,
                  "attempted": 2,
              })
        check("multi data lands under Data and quality note stays outside",
              (data_folder / "AAA.csv").read_bytes() == b"AAA-DATA\n"
              and data_folder == folder / "Data"
              and data_folder == Path(result["data_folder"])
              and Path(result["note_path"]).parent == folder
              and Path(result["note_path"]).is_file())
        check("current health report is copied byte-identically beside Data",
              Path(result["health_report"]) == folder / "health_report.json"
              and Path(result["health_report"]).read_bytes()
              == (bank / "_health_report.json").read_bytes())
        note = Path(result["note_path"]).read_text(encoding="utf-8")
        check("note reports missing month and absent ticker",
              "Missing months: 2026-02" in note
              and "MISS: NOT_IN_BANK" in note)
        check("quality summary separates current and absent evidence",
              result["quality_summary"]["CLEAN"] == 2
              and result["quality_summary"]["NO_CURRENT_EVIDENCE"] == 1)
        check("progress is structured and renderer is usable",
              any(event.get("kind") == "item_progress" for event in events)
              and all(batch.progress_text(event) for event in events))

        collision = batch.run_batch(
            bank, ["AAA", "BBB", "MISS"], ["AAA", "BBB"], output,
            interval="1m", file_format="csv", sessions="rth",
            start="all", end="latest",
            filename_for=lambda ticker: f"{ticker}.csv",
            exporter=lambda ticker, target, _progress, _cancel: (
                target.write_bytes(ticker.encode("ascii")),
                {"rows": 1, "holes": []})[1],
            now=NOW, quality_kwargs=quality)
        check("same-second second batch never reuses the first folder",
              Path(collision["bundle_folder"]).name == folder.name + " (2)"
              and folder.is_dir())

        cancel_parent = project / "Cancel Before"
        cancel_parent.mkdir()
        cancel = threading.Event()
        cancel.set()
        calls = []
        cancelled = batch.run_batch(
            bank, ["AAA", "BBB"], ["AAA", "BBB"], cancel_parent,
            interval="1m", file_format="csv", sessions="rth",
            start="all", end="latest",
            filename_for=lambda ticker: f"{ticker}.csv",
            exporter=lambda *_args: calls.append(True), cancel=cancel,
            now=NOW, quality_kwargs=quality)
        check("cancel before first write calls no exporter",
              not calls and cancelled["summary"]["attempted"] == 0)
        check("cancel before first write retains the health-only bundle",
              cancelled["state"] == "CANCELLED"
              and cancelled["folder_removed"] is False
              and Path(cancelled["bundle_folder"]).is_dir()
              and Path(cancelled["health_report"]).is_file()
              and cancelled["note_path"] is None)

        after_parent = project / "Cancel After"
        after_parent.mkdir()
        after_event = threading.Event()

        def cancel_after_one(ticker, target, _progress, _cancel):
            target.write_bytes(ticker.encode("ascii"))
            after_event.set()
            return {"rows": 1, "holes": []}

        after = batch.run_batch(
            bank, ["AAA", "BBB"], ["AAA", "BBB"], after_parent,
            interval="1m", file_format="csv", sessions="rth",
            start="all", end="latest",
            filename_for=lambda ticker: f"{ticker}.csv",
            exporter=cancel_after_one, workers=1, cancel=after_event,
            now=NOW, quality_kwargs=quality)
        check("cancel after one file preserves completed work and note",
              after["state"] == "CANCELLED"
              and after["summary"]["files_written"] == 1
              and after["summary"]["cancelled"] == 1
              and Path(after["note_path"]).is_file()
              and Path(after["bundle_folder"]).is_dir())

        note_fail_parent = project / "Note Failure"
        note_fail_parent.mkdir()
        note_failed = batch.run_batch(
            bank, ["AAA", "BBB"], ["AAA", "BBB"], note_fail_parent,
            interval="1m", file_format="csv", sessions="rth",
            start="all", end="latest",
            filename_for=lambda ticker: f"{ticker}.csv",
            exporter=lambda ticker, target, _progress, _cancel: (
                target.write_bytes(ticker.encode("ascii")),
                {"rows": 1, "holes": []})[1],
            now=NOW, quality_kwargs={
                "run_logs_root": bank,
                "tier0_adapter": FakeTier0(),
            })
        check("note failure never rolls back completed data files",
              note_failed["note_path"] is None
              and bool(note_failed["note_error"])
              and len(list(Path(note_failed["data_folder"]).glob("*.csv"))) == 2
              and Path(note_failed["health_report"]).is_file())

        empty_fail_parent = project / "Empty Note Failure"
        empty_fail_parent.mkdir()
        empty_failed = batch.run_batch(
            bank, ["AAA", "BBB"], ["AAA", "BBB"], empty_fail_parent,
            interval="1m", file_format="csv", sessions="rth",
            start="all", end="latest",
            filename_for=lambda ticker: f"{ticker}.csv",
            exporter=lambda *_args: {"rows": 0, "holes": []},
            now=NOW, quality_kwargs={"run_logs_root": bank})
        check("empty data still leaves the delivered health bundle",
              empty_failed["folder_removed"] is False
              and Path(empty_failed["bundle_folder"]).is_dir()
              and Path(empty_failed["health_report"]).is_file())

        no_parent = project / "No Parent"
        calls.clear()
        expect_error("folder failure aborts before any exporter",
                     lambda: batch.run_batch(
                         bank, ["AAA", "BBB"], ["AAA", "BBB"], no_parent,
                         interval="1m", file_format="csv", sessions="rth",
                         start="all", end="latest",
                         filename_for=lambda ticker: f"{ticker}.csv",
                         exporter=lambda *_args: calls.append(True), now=NOW))
        check("folder failure wrote nothing", not calls and not no_parent.exists())
        before_health_failure = {path.name for path in output.iterdir()}
        original_ensure = batch.export_quality.ensure_current_health

        def fail_health(_root):
            raise RuntimeError("fixture health failure")

        batch.export_quality.ensure_current_health = fail_health
        try:
            expect_error("health failure aborts before any exporter",
                         lambda: batch.run_batch(
                             bank, ["AAA", "BBB"], ["AAA", "BBB"], output,
                             interval="1m", file_format="csv", sessions="rth",
                             start="all", end="latest",
                             filename_for=lambda ticker: f"{ticker}.csv",
                             exporter=lambda *_args: calls.append(True),
                             now=NOW))
        finally:
            batch.export_quality.ensure_current_health = original_ensure
        check("health failure removes its empty partial bundle",
              {path.name for path in output.iterdir()} == before_health_failure
              and not calls)
        expect_error("destination inside bank is refused before export",
                     lambda: batch.run_batch(
                         bank, ["AAA", "BBB"], ["AAA", "BBB"], bank,
                         interval="1m", file_format="csv", sessions="rth",
                         start="all", end="latest",
                         filename_for=lambda ticker: f"{ticker}.csv",
                         exporter=mixed_exporter, now=NOW))
        expect_error("duplicate output filenames fail preflight",
                     lambda: batch.run_batch(
                         bank, ["AAA", "BBB"], ["AAA", "BBB"], output,
                         interval="1m", file_format="csv", sessions="rth",
                         start="all", end="latest",
                         filename_for=lambda _ticker: "same.csv",
                         exporter=mixed_exporter, now=NOW))

        direct = output / "direct-single.csv"
        single = batch.run_batch(
            bank, ["AAA"], ["AAA"], output,
            interval="1m", file_format="csv", sessions="rth",
            start="all", end="latest", direct_target=direct,
            filename_for=lambda ticker: f"{ticker}.csv",
            exporter=lambda _ticker, target, _progress, _cancel: (
                target.write_bytes(b"single\n"), {"rows": 1})[1], now=NOW)
        check("single direct name moves inside one flat health bundle",
              not direct.exists()
              and (Path(single["data_folder"]) / direct.name).read_bytes()
              == b"single\n"
              and Path(single["data_folder"]) == Path(single["folder"])
              and Path(single["bundle_folder"]) == Path(single["folder"])
              and "1 ticker" in Path(single["bundle_folder"]).name
              and single["folder_created"] is True
              and Path(single["health_report"]).is_file()
              and single["note_path"] is None)
        expect_error("single data filename cannot replace the health report",
                     lambda: batch.run_batch(
                         bank, ["AAA"], ["AAA"], output,
                         interval="1m", file_format="json", sessions="rth",
                         start="all", end="latest",
                         filename_for=lambda _ticker: "health_report.json",
                         exporter=lambda *_args: {"rows": 0}, now=NOW))

        actual_parent = project / "Actual Designer"
        baseline = project / "Baseline Designer"
        actual_parent.mkdir()
        baseline.mkdir()
        spec = export_designer.default_spec()
        spec["range_preset"] = "furthest"
        designer_ranges = {}
        for ticker in ("AAA", "BBB"):
            start_date, end_date = export_designer.resolve_range(
                bank, ticker, "1m", ["rth"], "furthest")
            designer_ranges[ticker] = (str(start_date), str(end_date))
            export_designer.export_one(
                bank, ticker, dict(spec, start_date=start_date,
                                   end_date=end_date),
                baseline / export_designer.filename_for(ticker, spec))
        bank_before = tree_snapshot(bank)
        actual = batch.run_designer_batch(
            bank, ["AAA", "BBB"], ["AAA", "BBB"], actual_parent, spec,
            now=NOW, quality_kwargs=quality)
        actual_folder = Path(actual["bundle_folder"])
        actual_data = Path(actual["folder"])
        check("designer batch bytes equal direct export_one bytes",
              all((baseline / export_designer.filename_for(ticker, spec)).read_bytes()
                  == (actual_data / export_designer.filename_for(
                      ticker, spec)).read_bytes()
                  for ticker in ("AAA", "BBB")))
        check("designer batch adds Data plus sibling reports",
              actual_data == actual_folder / "Data"
              and len(list(actual_data.glob("*.csv"))) == 2
              and Path(actual["health_report"]).parent == actual_folder
              and Path(actual["note_path"]).parent == actual_folder)
        check("designer batch carries each resolved range into quality outcomes",
              all((row.get("start"), row.get("end"))
                  == designer_ranges[row["ticker"]]
                  for row in actual["results"]),
              str(actual["results"]))
        check("designer batch carries exact source intervals into quality outcomes",
              all(row.get("source_intervals") == ["1m"]
                  for row in actual["results"]), str(actual["results"]))
        check("complete designer batch leaves bank bytes/metadata unchanged",
              tree_snapshot(bank) == bank_before)

        aux_bank = project / "Aux Bank"
        aux_logs = project / "Aux Run Logs"
        aux_parent = project / "Aux Designer"
        for path in (aux_bank, aux_logs, aux_parent):
            path.mkdir()
        seed_ticker(aux_bank, "AUXA", 0.0)
        seed_ticker(aux_bank, "AUXB", 20.0)
        seed_ratio(aux_bank, "AUXA", [
            dt.datetime(2026, 1, 2, 9, 30),
        ])
        seed_ratio(aux_bank, "AUXB", [
            dt.datetime(2026, 1, 2, 10, 0),
        ])
        for ticker in ("AUXA", "AUXB"):
            vol_value_audit.record_settled(
                aux_bank, ticker, "1m-iv", "2026-01-02", 0.3,
                reason="jump_up", run="batch-selftest",
                confirmed="2026-07-22")
        aux_spec = export_designer.default_spec()
        aux_spec.update({
            "range_preset": "furthest",
            "columns": ["timestamp", "close", "iv"],
        })
        aux_result = batch.run_designer_batch(
            aux_bank, ["AUXA", "AUXB"], ["AUXA", "AUXB"],
            aux_parent, aux_spec, workers=1, now=NOW,
            quality_kwargs={
                "run_logs_root": aux_logs,
                "tier0_adapter": FakeTier0(),
            })
        aux_by_ticker = {
            row["ticker"]: row for row in aux_result["results"]
        }
        aux_note = Path(aux_result["note_path"]).read_text(encoding="utf-8")
        check("actual Designer batch reports only ratio cells truly written",
              aux_by_ticker["AUXA"]["source_intervals"]
              == ["1m", "1m-iv"]
              and aux_by_ticker["AUXB"]["source_intervals"] == ["1m"]
              and "AUXA 1m-iv 2026-01-02" in aux_note
              and "AUXB 1m-iv 2026-01-02" not in aux_note,
              str((aux_by_ticker, aux_note)))
        classic_unmatched = export_designer.export_one(
            aux_bank, "AUXB", dict(
                aux_spec, start_date=dt.date(2026, 1, 2),
                end_date=dt.date(2026, 1, 5), date_format="custom",
                date_custom="%Y-%m-%d %H:%M:%S"),
            project / "aux-unmatched-classic.csv")
        parquet_unmatched = export_designer.export_one(
            aux_bank, "AUXB", dict(
                aux_spec, start_date=dt.date(2026, 1, 2),
                end_date=dt.date(2026, 1, 5), file_type="parquet"),
            project / "aux-unmatched-fast.parquet")
        check("classic and Parquet paths also exclude all-blank ratio sources",
              classic_unmatched["source_intervals"] == ["1m"]
              and parquet_unmatched["source_intervals"] == ["1m"],
              str((classic_unmatched, parquet_unmatched)))

        storage_parent = project / "Actual Storage"
        storage_parent.mkdir()
        manual = project / "manual-storage.csv"
        manual_spec = export_designer.default_spec()
        manual_spec.update({
            "base_interval": "1m", "sessions": ["rth"],
            "start_date": dt.date(2026, 1, 1),
            "end_date": dt.date(2026, 1, 31),
            "file_type": "csv", "storage_format": True,
        })
        export_designer.export_one(bank, "AAA", manual_spec, manual)
        storage_result = batch.run_storage_batch(
            bank, ["AAA"], storage_parent, interval="1m", mode="dates",
            start_date=dt.date(2026, 1, 1), end_date=dt.date(2026, 1, 31),
            hours_mode="Regular hours", file_format="csv", now=NOW)
        storage_file = next(Path(storage_result["data_folder"]).glob("AAA_*.csv"))
        check("moved Storage Export pipeline remains byte-identical",
              storage_file.read_bytes() == manual.read_bytes()
              and Path(storage_result["health_report"]).is_file()
              and storage_result["note_path"] is None)
        check("storage batch preserves its resolved per-ticker range",
              storage_result["results"][0].get("start") == "2026-01-01"
              and storage_result["results"][0].get("end") == "2026-01-31",
              str(storage_result["results"]))

        storage_progress = []
        with aggregate_rate(0.0):
            storage_progress_result = batch.run_storage_batch(
                bank, ["AAA"], storage_parent, interval="1m", mode="dates",
                start_date=dt.date(2026, 1, 1),
                end_date=dt.date(2026, 3, 31),
                hours_mode="Regular hours", file_format="csv", now=NOW,
                progress=storage_progress.append)
        storage_aggregates = [
            event for event in storage_progress
            if event.get("kind") == "aggregate"
        ]
        storage_raw_details = [
            event.get("detail") for event in storage_progress
            if event.get("kind") == "item_progress"
        ]
        check("legacy Storage Export gets smooth aggregate month motion",
              {event["units_done"] for event in storage_aggregates}
              == {0, 1, 2, 3}
              and storage_aggregates[-1] == {
                  "kind": "aggregate", "units_done": 3,
                  "units_total": 3, "files_done": 1, "files_total": 1,
                  "active": [],
              }
              and storage_progress_result["state"] == "COMPLETE",
              str(storage_aggregates))
        check("legacy raw progress keeps its single historical log detail",
              storage_raw_details == [
                  "stitching AAA 1m 2026-01-01..2026-03-31",
              ], str(storage_raw_details))

        manual_parquet = project / "manual-storage.parquet"
        export_csv.export_combined_csv(
            bank, "AAA", "1m", dt.date(2026, 1, 1),
            dt.date(2026, 3, 31), manual_parquet, fmt="parquet")
        parquet_events = []
        with aggregate_rate(0.0):
            parquet_result = batch.run_storage_batch(
                bank, ["AAA"], storage_parent, interval="1m", mode="dates",
                start_date=dt.date(2026, 1, 1),
                end_date=dt.date(2026, 3, 31),
                hours_mode="Regular hours", file_format="parquet", now=NOW,
                progress=parquet_events.append)
        parquet_file = Path(parquet_result["data_folder"]) / (
            "AAA_1m_2026-01-01_2026-03-31.parquet")
        parquet_aggregates = [
            event for event in parquet_events
            if event.get("kind") == "aggregate"
        ]
        check("legacy Parquet export keeps bytes and gains month motion",
              parquet_result["state"] == "COMPLETE"
              and parquet_file.read_bytes() == manual_parquet.read_bytes()
              and {event["units_done"] for event in parquet_aggregates}
              == {0, 1, 2, 3}
              and parquet_aggregates[-1]["units_done"] == 3,
              str(parquet_aggregates))

        combined_parquet_events = []
        with aggregate_rate(0.0):
            batch.run_storage_batch(
                bank, ["AAA"], storage_parent, interval="1m", mode="dates",
                start_date=dt.date(2026, 1, 1),
                end_date=dt.date(2026, 3, 31),
                hours_mode="Regular + extended (one file)",
                file_format="parquet", now=NOW,
                progress=combined_parquet_events.append)
        combined_parquet_aggregates = [
            event for event in combined_parquet_events
            if event.get("kind") == "aggregate"
        ]
        check("combined-session Parquet spreads units across session reads",
              {event["units_done"] for event in combined_parquet_aggregates}
              == {0, 1, 2, 3}
              and combined_parquet_aggregates[-1]["units_done"] == 3,
              str(combined_parquet_aggregates))

        mid_cancel_events = []

        def mid_cancel_exporter(_ticker, _target, item_progress, _cancel):
            for month_index in (1, 2, 3):
                item_progress({
                    "month_index": month_index,
                    "month_total": 4,
                    "month": f"2026-0{month_index}",
                })
            raise export_designer.ExportCancelled("fixture cancellation")

        def mutating_progress(event):
            mid_cancel_events.append(event)
            detail = event.get("detail")
            if event.get("kind") == "item_progress" and isinstance(detail, dict):
                detail["month_index"] = 999

        with aggregate_rate(0.0):
            mid_cancel = batch.run_batch(
                bank, ["AAA"], ["AAA"], output,
                interval="1m", file_format="csv", sessions="rth",
                start="2026-01-01", end="2026-04-30",
                filename_for=lambda ticker: f"{ticker}-mid-cancel.csv",
                exporter=mid_cancel_exporter,
                direct_target=output / "mid-cancel.csv",
                progress=mutating_progress, now=NOW,
                planned_months_for=lambda _ticker: 4)
        mid_cancel_final = [
            event for event in mid_cancel_events
            if event.get("kind") == "aggregate"
        ][-1]
        check("mid-ticker cancellation freezes mutation-safe completed units",
              mid_cancel["state"] == "CANCELLED"
              and mid_cancel_final["units_done"] == 2
              and mid_cancel_final["units_total"] == 4
              and mid_cancel_final["files_done"] == 1
              and mid_cancel_final["active"] == [],
              str(mid_cancel_final))

        fallback_events = []

        def fallback_plan(ticker):
            if ticker == "BBB":
                raise storage.StorageError("fixture planner failure")
            return 5

        with aggregate_rate(0.0):
            fallback = batch.run_batch(
                bank, ["AAA", "BBB"], ["AAA", "BBB"], output,
                interval="1m", file_format="csv", sessions="rth",
                start="fixture", end="fixture",
                filename_for=lambda ticker: f"{ticker}-fallback.csv",
                exporter=lambda ticker, _target, _progress, _cancel: (
                    {"error": "fixture export failure"} if ticker == "AAA"
                    else {"rows": 0, "skipped": "fixture empty"}),
                progress=fallback_events.append, now=NOW,
                quality_kwargs=quality,
                planned_months_for=fallback_plan)
        fallback_aggregates = [
            event for event in fallback_events
            if event.get("kind") == "aggregate"
        ]
        check("planner failures fall back to one and settled outcomes snap",
              fallback["state"] == "PARTIAL"
              and {event["units_total"] for event in fallback_aggregates}
              == {6}
              and fallback_aggregates[-1]["units_done"] == 6
              and fallback_aggregates[-1]["files_done"] == 2,
              str(fallback_aggregates))

        active_tickers = ["AAA", "BBB", "CCC", "DDD"]
        active_events = []
        start_barrier = threading.Barrier(len(active_tickers))

        def concurrent_exporter(ticker, target, item_progress, _cancel):
            item_progress({"month_index": 1, "month_total": 2})
            start_barrier.wait(timeout=5)
            item_progress({"month_index": 2, "month_total": 2})
            target.write_bytes(ticker.encode("ascii"))
            return {"rows": 1}

        with aggregate_rate(0.0):
            batch.run_batch(
                bank, active_tickers, active_tickers, output,
                interval="1m", file_format="csv", sessions="rth",
                start="fixture", end="fixture",
                filename_for=lambda ticker: f"{ticker}-active.csv",
                exporter=concurrent_exporter, workers=4,
                progress=active_events.append, now=NOW,
                quality_kwargs=quality,
                planned_months_for=lambda _ticker: 2)
        active_aggregates = [
            event for event in active_events
            if event.get("kind") == "aggregate"
        ]
        check("parallel aggregate delivery is ordered with capped active state",
              all(0 <= event["units_done"] <= event["units_total"] == 8
                  and 0 <= event["files_done"] <= event["files_total"] == 4
                  and len(event["active"]) <= 3
                  and event["active"] == [
                      ticker for ticker in active_tickers
                      if ticker in event["active"]
                  ][:3]
                  for event in active_aggregates)
              and any(event["active"] == ["AAA", "BBB", "CCC"]
                      for event in active_aggregates)
              and all(later["units_done"] >= earlier["units_done"]
                      for earlier, later in zip(
                          active_aggregates, active_aggregates[1:]))
              and active_aggregates[-1]["active"] == [],
              str(active_aggregates))

        coalesced_events = []

        def coalesced_exporter(_ticker, target, item_progress, _cancel):
            for month_index in range(1, 5):
                item_progress({
                    "month_index": month_index, "month_total": 4,
                })
            target.write_bytes(b"coalesced\n")
            return {"rows": 1}

        with aggregate_rate(3_600.0):
            batch.run_batch(
                bank, ["AAA"], ["AAA"], output,
                interval="1m", file_format="csv", sessions="rth",
                start="fixture", end="fixture",
                filename_for=lambda _ticker: "coalesced.csv",
                exporter=coalesced_exporter,
                direct_target=output / "coalesced.csv",
                progress=coalesced_events.append, now=NOW,
                planned_months_for=lambda _ticker: 4)
        coalesced_aggregates = [
            event for event in coalesced_events
            if event.get("kind") == "aggregate"
        ]
        coalesced_count = len(coalesced_events)
        time.sleep(0.05)
        check("coalescing keeps initial and synchronous forced terminal only",
              coalesced_aggregates == [
                  {"kind": "aggregate", "units_done": 0,
                   "units_total": 4, "files_done": 0, "files_total": 1,
                   "active": []},
                  {"kind": "aggregate", "units_done": 4,
                   "units_total": 4, "files_done": 1, "files_total": 1,
                   "active": []},
              ] and len(coalesced_events) == coalesced_count,
              str(coalesced_aggregates))

        check("aggregate renderer is explicit, stable, and forward-rounded",
              batch.progress_text({
                  "kind": "aggregate", "units_done": 3,
                  "units_total": 7, "files_done": 1, "files_total": 2,
                  "active": ["AAA", "BBB"],
              }) == ("42% \u2014 months 3/7 \u2014 files 1/2 \u2014 "
                      "exporting AAA, BBB"))

        source = Path(batch.__file__).read_text(encoding="utf-8")
        imports = set()
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imports.update(alias.name.split(".")[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imports.add(node.module.split(".")[0])
        check("shared orchestrator is tkinter-free and network-free",
              "tkinter" not in imports
              and imports.isdisjoint({"requests", "urllib", "socket", "ib_async"}))

        display_source = (Path(batch.__file__).parent.parent / "display_data.py").read_text(
            encoding="utf-8")
        check("both GUI exporters use the shared batch orchestrator",
              "export_batch.run_storage_batch(" in display_source
              and "export_batch.run_designer_batch(" in display_source
              and "def _export_combined_pipeline(" not in display_source)
        check("both GUI exporters refresh current health before export",
              display_source.count(
                  "export_quality.ensure_current_health(root_dir)") == 2
              and display_source.count(
                  "Checking bank health for export") == 2)
        check("both GUI completion surfaces report the outer bundle",
              display_source.count('payload.get("bundle_folder")') == 2)
        display_tree = ast.parse(display_source)
        display_app = next(
            node for node in display_tree.body
            if isinstance(node, ast.ClassDef) and node.name == "DataViewerApp")
        designer_start = next(
            node for node in display_app.body
            if isinstance(node, ast.FunctionDef) and node.name == "_exd_export")
        designer_source = ast.get_source_segment(display_source, designer_start)
        check("single Designer exports now choose a bundle parent folder",
              "filedialog.askdirectory(" in designer_source
              and "asksaveasfilename" not in designer_source
              and "direct_target" not in designer_source)

        fake_ttk = _FakeTtk()
        gui_bits, app_node = _load_export_gui_bits(fake_ttk)
        ui = _fake_export_ui(gui_bits)

        inventory_bank = project / "Inventory Bank"
        inventory_ticker = inventory_bank / "VOLONLY"
        inventory_ticker.mkdir(parents=True)
        inventory_manifest = storage.new_manifest("VOLONLY", "VOLONLY")
        storage.manifest_months(inventory_manifest, "1d-iv")
        storage.save_manifest(inventory_ticker, inventory_manifest)
        ui._storage_root = inventory_bank
        ui._storage_cached_known_series = lambda: None
        inventory_tickers, inventory_intervals = ui._exd_bank_inventory()
        check("Export Designer GUI inventory keeps a kind-only ticker",
              inventory_tickers == ["VOLONLY"]
              and inventory_intervals == ["1m"],
              f"tickers={inventory_tickers}, intervals={inventory_intervals}")

        for prefix in ("exd", "export"):
            setattr(ui, f"_{prefix}_status", _FakeVar())
            setattr(ui, f"_{prefix}_activity", _FakeVar())
            ui._export_progress_surface(
                object(), prefix, getattr(ui, f"_{prefix}_status"),
                getattr(ui, f"_{prefix}_activity"))
        bars = [widget for widget in fake_ttk.created
                if widget.kind == "Progressbar"]
        check("both export surfaces create determinate zeroed progress bars",
              len(bars) == 2
              and all(bar.options.get("mode") == "determinate"
                      and bar.options.get("maximum") == 1
                      and bar.options.get("value") == 0
                      for bar in bars))

        app_methods = {
            node.name: node for node in app_node.body
            if isinstance(node, ast.FunctionDef)
        }

        storage_toolbar = ast.unparse(app_methods["_build_storage_tab"])
        add_stock = storage_toolbar.find(
            "self._storage_find_btn = ttk.Button(bar, text='Add stock'")
        repair_data = storage_toolbar.find(
            "self._storage_fixdata_btn = ttk.Button(bar, text='Repair data…'")
        second_row = storage_toolbar.find("bar2 = ttk.Frame(")
        export_button = storage_toolbar.find(
            "self._storage_export_btn = ttk.Button(bar2, text='Export…'")
        check("Storage toolbar pins Repair beside Add stock and Export right",
              -1 not in (add_stock, repair_data, second_row, export_button)
              and add_stock < repair_data < second_row < export_button
              and "self._storage_export_btn.pack(side=tk.RIGHT)"
              in storage_toolbar)

        def called(method_name, target):
            return any(
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == target
                for node in ast.walk(app_methods[method_name]))

        check("both dialogs and polls share one export progress contract",
              called("_storage_export_dialog", "_export_progress_surface")
              and called("_storage_exd_open", "_export_progress_surface")
              and called("_storage_export_poll", "_export_progress_apply")
              and called("_exd_export_poll", "_export_progress_apply")
              and called("_storage_export_run", "_export_progress_reset")
              and called("_storage_export_run", "_export_progress_pulse_start")
              and called("_exd_export", "_export_progress_reset")
              and called("_exd_export", "_export_progress_pulse_start")
              and called("_exd_cancel_export", "_export_progress_freeze")
              and called("_storage_export_done", "_export_progress_finish")
              and called("_exd_export_done", "_export_progress_finish")
              and called("_exd_close", "_export_progress_finish"))

        ui._export_progress_reset("exd", "Exporting 6 file(s)…", now=0)
        exd_bar = ui._exd_progress
        before_bar = len(exd_bar.config_history)
        before_summary = ui._exd_status.value
        raw_event = {
            "kind": "item_progress", "ticker": "AAA", "index": 1,
            "total": 6,
            "detail": {"month_index": 2, "month_total": 8,
                       "month": "2026-02"},
        }
        raw_result = ui._export_progress_apply("exd", raw_event, now=1)
        check("raw export activity cannot move the determinate bar or summary",
              raw_result == "activity"
              and len(exd_bar.config_history) == before_bar
              and ui._exd_status.value == before_summary
              and "AAA" in ui._exd_activity.value)

        def aggregate(done, *, total=63, files_done=1, files_total=6):
            return {
                "kind": "aggregate", "units_done": done,
                "units_total": total, "files_done": files_done,
                "files_total": files_total, "active": ["AAA"],
            }

        ui._export_progress_apply("exd", aggregate(0, files_done=0), now=0)
        ui._export_progress_apply("exd", aggregate(7), now=7)
        stable = ui._exd_status.value
        ui._export_progress_apply("exd", dict(raw_event, ticker="BBB"), now=7.5)
        after_activity = ui._exd_status.value
        ui._export_progress_apply("exd", aggregate(5), now=8)
        after_stale = ui._exd_status.value
        ui._export_progress_apply(
            "exd", aggregate(15, files_done=2), now=15)
        values = [entry["value"] for entry in exd_bar.config_history
                  if entry.get("maximum") == 63]
        check("aggregate bar is fixed-denominator, forward-only, and ETA-backed",
              values == [0, 7, 15]
              and "months 15/63" in ui._exd_status.value
              and "files 2/6" in ui._exd_status.value
              and "left" in ui._exd_status.value
              and "AAA" not in ui._exd_status.value,
              f"values={values}, summary={ui._exd_status.value!r}")
        check("activity changes and stale aggregates leave stable progress intact",
              stable == after_activity == after_stale
              and "BBB" in ui._exd_activity.value)

        mailbox_queue = queue.Queue()
        mailbox = gui_bits["_ExportProgressCoalescer"](mailbox_queue)
        check("coalescer queues only one token per pending channel",
              mailbox.post(aggregate(7))
              and not mailbox.post(aggregate(15, files_done=2))
              and mailbox.post(raw_event)
              and not mailbox.post(dict(raw_event, ticker="BBB")))
        mailbox.post(aggregate(63, files_done=6), force=True)
        mailbox_queue.put(("done", {"state": "COMPLETE"}))
        trace = []
        while True:
            channel, payload = mailbox_queue.get_nowait()
            if channel == "done":
                trace.append(("done", payload))
                break
            trace.append((channel, payload.take(channel)))
        aggregate_events = [event for channel, event in trace
                            if channel == "aggregate"]
        check("latest terminal aggregate drains before done without loss",
              len(aggregate_events) == 1
              and aggregate_events[0]["units_done"] == 63
              and aggregate_events[0]["files_done"] == 6
              and next(i for i, item in enumerate(trace)
                       if item[0] == "aggregate")
              < next(i for i, item in enumerate(trace)
                     if item[0] == "done"),
              str(trace))

        class FakeRoot:
            def __init__(self):
                self.after_calls = []
                self.cancelled = []

            def after(self, delay, callback):
                self.after_calls.append((delay, callback))
                return len(self.after_calls)

            def after_cancel(self, handle):
                self.cancelled.append(handle)

        class FakeWindow:
            @staticmethod
            def winfo_exists():
                return True

        designer_ui = _fake_export_ui(gui_bits)
        designer_ui.root = FakeRoot()
        designer_ui._exd_win = FakeWindow()
        designer_ui._exd_cancel_ev = threading.Event()
        designer_ui._storage_exd_running = True
        designer_trace = []
        designer_ui._export_progress_apply = (
            lambda prefix, event: designer_trace.append(
                ("apply", prefix, event["kind"])))
        designer_ui._exd_export_done = (
            lambda payload: designer_trace.append(("done", payload)))
        designer_q = queue.Queue()
        designer_mailbox = gui_bits["_ExportProgressCoalescer"](designer_q)
        designer_mailbox.post(aggregate(63, files_done=6), force=True)
        designer_mailbox.post(raw_event)
        designer_q.put(("done", "designer-result"))
        designer_ui._exd_q = designer_q
        designer_ui._exd_export_poll()
        check("Designer poll applies structured channels before done on Tk side",
              designer_trace == [
                  ("apply", "exd", "aggregate"),
                  ("apply", "exd", "item_progress"),
                  ("done", "designer-result"),
              ] and designer_ui._exd_q is None,
              str(designer_trace))

        legacy_ui = _fake_export_ui(gui_bits)
        legacy_ui.root = FakeRoot()
        legacy_trace = []
        legacy_logs = []
        legacy_ui._export_progress_apply = (
            lambda prefix, event: legacy_trace.append(
                ("apply", prefix,
                 event.get("kind") if isinstance(event, dict) else "text")))
        legacy_ui._storage_export_say = legacy_logs.append
        legacy_ui._storage_export_done = (
            lambda payload: legacy_trace.append(("done", payload)))
        legacy_q = queue.Queue()
        legacy_q.put(("progress", aggregate(4, total=8, files_done=1,
                                             files_total=2)))
        legacy_q.put(("progress", "stitching AAA"))
        legacy_q.put(("done", "legacy-result"))
        legacy_ui._storage_export_q = legacy_q
        legacy_ui._storage_export_poll()
        check("legacy poll adds the bar without changing progress log lines",
              legacy_trace == [
                  ("apply", "export", "aggregate"),
                  ("apply", "export", "text"),
                  ("done", "legacy-result"),
              ]
              and legacy_logs == [
                  batch.progress_text(aggregate(
                      4, total=8, files_done=1, files_total=2)),
                  "stitching AAA",
              ], str((legacy_trace, legacy_logs)))

        ui._export_progress_reset("exd", "Exporting", now=0)
        ui._export_progress_apply("exd", aggregate(23, files_done=2), now=23)
        ui._exd_cancel_ev = threading.Event()
        ui._exd_cancel_btn = _FakeWidget("Button")
        ui._exd_cancel_export()
        frozen_result = ui._export_progress_apply(
            "exd", aggregate(23, files_done=6), now=63)
        frozen_value = ui._exd_progress.options["value"]
        frozen_copy = ui._exd_status.value
        cancel_set = ui._exd_cancel_ev.is_set()
        cancel_disabled = (
            ui._exd_cancel_btn.options.get("state") == _FakeTk.DISABLED)
        ui._export_progress_reset("exd", "Exporting", now=0)
        ui._export_progress_apply("exd", aggregate(23, files_done=2), now=23)
        ui._exd_cancel_ev = threading.Event()
        ui._exd_cancel_btn = _FakeWidget("Button")
        ui._exd_cancel_export()
        late_result = ui._export_progress_apply(
            "exd", aggregate(63, files_done=6), now=63)
        late_value = ui._exd_progress.options["value"]
        reset = ui._export_progress_reset("exd", "Exporting again", now=100)
        check("cancel freezes partial progress and the next run resets cleanly",
              frozen_result == "frozen" and frozen_value == 23
              and frozen_copy.startswith("Cancelling export")
              and cancel_set and cancel_disabled
              and late_result == "aggregate" and late_value == 63
              and ui._exd_progress.options["maximum"] == 1
              and ui._exd_progress.options["value"] == 0
              and not reset["frozen"])

        def cancel_then_poll(terminal):
            candidate = _fake_export_ui(gui_bits)
            candidate._exd_status = _FakeVar()
            candidate._exd_activity = _FakeVar()
            candidate._export_progress_surface(
                object(), "exd", candidate._exd_status,
                candidate._exd_activity)
            started = time.monotonic()
            candidate._export_progress_reset("exd", "Exporting", now=started)
            candidate._export_progress_apply(
                "exd", aggregate(23, files_done=2), now=started + 1)
            candidate.root = FakeRoot()
            candidate._exd_win = FakeWindow()
            candidate._exd_cancel_ev = threading.Event()
            candidate._exd_cancel_btn = _FakeWidget("Button")
            candidate._storage_exd_running = True
            candidate_done = []
            candidate._exd_export_done = candidate_done.append
            candidate_q = queue.Queue()
            candidate_mailbox = gui_bits[
                "_ExportProgressCoalescer"](candidate_q)
            candidate_mailbox.post(terminal, force=True)
            candidate_q.put(("done", "result"))
            candidate._exd_q = candidate_q
            candidate._exd_cancel_export()
            candidate._exd_export_poll()
            return (candidate._exd_progress.options["value"],
                    candidate._exd_status.value, candidate_done)

        partial_poll = cancel_then_poll(aggregate(23, files_done=6))
        complete_poll = cancel_then_poll(aggregate(63, files_done=6))
        check("late Cancel reconciles COMPLETE but preserves cancelled partial",
              partial_poll[0] == 23
              and partial_poll[1].startswith("Cancelling export")
              and partial_poll[2] == ["result"]
              and complete_poll[0] == 63
              and complete_poll[1].startswith("100%")
              and complete_poll[2] == ["result"],
              str((partial_poll, complete_poll)))

        pulse_ui = _fake_export_ui(gui_bits)
        pulse_ui.root = FakeRoot()
        pulse_ui._exd_status = _FakeVar()
        pulse_ui._exd_activity = _FakeVar()
        pulse_ui._export_progress_surface(
            object(), "exd", pulse_ui._exd_status,
            pulse_ui._exd_activity)
        started = time.monotonic() - 2.0
        pulse_ui._export_progress_reset("exd", "Exporting", now=started)
        bar_updates = len(pulse_ui._exd_progress.config_history)
        pulse_ui._export_progress_pulse_start("exd")
        first_handle = pulse_ui._exd_progress_pulse_after
        first_callback = pulse_ui.root.after_calls[-1][1]
        first_callback()
        check("liveness pulse schedules at 400ms and changes text only",
              pulse_ui.root.after_calls[0][0] == 400
              and first_handle == 1
              and "elapsed" in pulse_ui._exd_status.value
              and len(pulse_ui._exd_progress.config_history) == bar_updates)

        pulse_ui._export_progress_apply(
            "exd", aggregate(8, total=8, files_done=2, files_total=2))
        pulse_ui._export_progress_pulse("exd")
        finalizing = pulse_ui._exd_status.value
        pulse_ui._export_progress_freeze("exd")
        pulse_ui._export_progress_pulse("exd")
        cancelling = pulse_ui._exd_status.value
        pending = pulse_ui._exd_progress_pulse_after
        pulse_ui._export_progress_finish("exd")
        pulse_ui._exd_status.set("Export complete")
        pulse_ui._export_progress_pulse("exd")
        check("pulse exposes finalizing/cancel and preserves terminal summary",
              "finalizing" in finalizing
              and cancelling.startswith("Cancelling at the next safe boundary")
              and pending in pulse_ui.root.cancelled
              and pulse_ui._exd_status.value == "Export complete"
              and pulse_ui._exd_progress_pulse_after is None
              and len(pulse_ui._exd_progress.config_history) == bar_updates + 1)

        ui._export_progress_reset("export", "Preparing", now=0)
        legacy_before = len(ui._export_progress.config_history)
        ui._export_progress_apply("export", "stitching AAA", now=1)
        ui._export_progress_apply(
            "export", aggregate(4, total=8, files_done=1,
                                files_total=2), now=4)
        check("legacy export shares aggregate-only bar semantics",
              len(ui._export_progress.config_history) == legacy_before + 1
              and ui._export_progress.options["maximum"] == 8
              and ui._export_progress.options["value"] == 4
              and ui._export_activity.value == "stitching AAA")

        post_progress = next(
            node for node in ast.walk(app_methods["_exd_export"])
            if isinstance(node, ast.FunctionDef)
            and node.name == "_post_progress")
        post_text = ast.unparse(post_progress)
        check("Designer worker callback only posts structured mailbox events",
              "progress_mailbox.post" in post_text
              and "_export_progress_apply" not in post_text
              and ".set(" not in post_text
              and ".configure(" not in post_text)
    finally:
        shutil.rmtree(project, ignore_errors=True)


if __name__ == "__main__":
    run()
    if FAILURES:
        print(f"FAILED: {len(FAILURES)}/{COUNT[0]} checks: "
              + ", ".join(FAILURES))
        raise SystemExit(1)
    print(f"ALL PASS ({COUNT[0]}/{COUNT[0]}; temp bank; no live export)")
