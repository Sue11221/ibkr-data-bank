"""Profile, safety, parity, and exit-code tests for the export command."""

from __future__ import annotations

import ast
import datetime as dt
import hashlib
import io
import json
import shutil
import sys
import tempfile
import threading
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parent))

import export_batch  # noqa: E402
import export_cli  # noqa: E402
import export_designer  # noqa: E402
import export_quality  # noqa: E402
import operation_gate  # noqa: E402
import stock_storage as storage  # noqa: E402


FAILURES = []
COUNT = [0]
NOW = dt.datetime(
    2026, 7, 13, 14, 5, 6,
    tzinfo=dt.timezone(dt.timedelta(hours=-4)))


def check(name, condition, detail=""):
    COUNT[0] += 1
    print(f"[{'PASS' if condition else 'FAIL'}] {name}"
          + (f" -- {detail}" if detail and not condition else ""))
    if not condition:
        FAILURES.append(name)


def expect_error(name, fn, contains=None):
    try:
        fn()
    except export_cli.ExportCommandError as exc:
        check(name, contains is None or contains in str(exc), str(exc))
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
    manifest["conid"] = 20_000 + sum(ord(char) for char in ticker)
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


def write_ws7(path, fingerprints):
    rows = [
        {"ticker": ticker, "manifest_fingerprint": fingerprint,
         "verdict": "CLEAN"}
        for ticker, fingerprint in sorted(fingerprints.items())
    ]
    counts = {verdict: 0 for verdict in export_quality.WS7_VERDICTS}
    counts["CLEAN"] = len(rows)
    payload = {
        "kind": export_quality.WS7_KIND,
        "version": export_quality.WS7_VERSION,
        "report_only": True,
        "provider": "stockanalysis",
        "reference_range": "Max",
        "finished_at": "2026-07-13T17:00:00+00:00",
        "params": {"ticker_count": len(rows)},
        "counts": counts,
        "rows": rows,
    }
    Path(path).write_text(
        json.dumps(payload, sort_keys=True, separators=(",", ":")),
        encoding="utf-8")


class FakeTier0:
    def repair_queue(self, *, cursor=0, limit=100):
        return {
            "kind": "tier0_repair_queue",
            "asof": "2026-07-13T13:00:00-04:00",
            "evidence_current": True,
            "queue": [],
            "pagination": {
                "cursor": cursor, "limit": limit, "returned": 0,
                "next_cursor": None,
            },
            "network": False,
            "written": False,
            "evidence": {
                "fingerprint": "b" * 64,
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
        raise AssertionError(f"unexpected detail query: {ticker}")


def tree_snapshot(root):
    rows = []
    for path in sorted(item for item in Path(root).rglob("*") if item.is_file()):
        raw = path.read_bytes()
        stat = path.stat()
        rows.append((
            path.relative_to(root).as_posix(), len(raw), stat.st_mtime_ns,
            sha(raw)))
    return rows


def base_profile(destination, tickers=None):
    profile = export_cli.profile_template()
    profile["destination"] = str(Path(destination).resolve())
    profile["tickers"] = tickers if tickers is not None else ["AAA", "BBB"]
    profile["workers"] = 2
    return profile


def write_profile(path, profile):
    Path(path).write_text(
        json.dumps(profile, indent=2) + "\n", encoding="utf-8")


def ample_disk(_path):
    return SimpleNamespace(free=20 * 1024 ** 3)


def available_gate():
    return {"available": True, "path": "fixture-gate"}


def result_stub(state="COMPLETE", *, note_error=None, cancelled=0):
    return {
        "state": state,
        "folder": "fixture-output",
        "summary": {
            "files_written": 2 if state == "COMPLETE" else 1,
            "rows": 6 if state == "COMPLETE" else 3,
            "cancelled": cancelled,
        },
        "results": [],
        "note_path": None,
        "note_error": note_error,
        "elapsed_seconds": 0.25,
    }


def run():
    project = Path(tempfile.mkdtemp(prefix="export-cli-selftest-"))
    bank = project / storage.STORAGE_DIR_NAME
    logs = project / "Run Logs"
    output_a = project / "Output A"
    output_b = project / "Output B"
    output_c = project / "Output C"
    for path in (bank, logs, output_a, output_b, output_c):
        path.mkdir(parents=True)
    fingerprints = {
        "AAA": seed_ticker(bank, "AAA", 0.0),
        "BBB": seed_ticker(bank, "BBB", 20.0),
    }
    ws7 = logs / "external-sweep-fixture-v2.json"
    write_ws7(ws7, fingerprints)
    quality = {
        "run_logs_root": logs,
        "ws7_paths": [ws7],
        "tier0_adapter": FakeTier0(),
    }

    try:
        example = (Path(export_cli.__file__).parent.parent
                   / "export_profile.example.json")
        check("tracked example equals the --init template",
              json.loads(example.read_text(encoding="utf-8"))
              == export_cli.profile_template())

        initialized = project / "initialized.json"
        export_cli.write_profile_template(initialized)
        check("--init helper writes valid JSON round-trip",
              export_cli.load_profile(initialized)
              == export_cli.profile_template())
        expect_error("--init never overwrites an existing profile",
                     lambda: export_cli.write_profile_template(initialized),
                     "refusing to overwrite")
        expect_error("generated empty destination fails closed",
                     lambda: export_cli.validate_profile(
                         export_cli.load_profile(initialized),
                         storage_root=bank), "destination is empty")

        missing_profile = project / "missing.json"
        expect_error("missing profile names --init recovery",
                     lambda: export_cli.load_profile(missing_profile), "--init")
        malformed = project / "malformed.json"
        malformed.write_text("{no", encoding="utf-8")
        expect_error("malformed JSON is rejected",
                     lambda: export_cli.load_profile(malformed), "valid UTF-8 JSON")
        duplicate = project / "duplicate.json"
        duplicate.write_text(
            '{"version":1,"version":1}', encoding="utf-8")
        expect_error("duplicate JSON keys are rejected",
                     lambda: export_cli.load_profile(duplicate), "duplicate JSON key")
        nonfinite = project / "nonfinite.json"
        nonfinite.write_text('{"x":NaN}', encoding="utf-8")
        expect_error("non-finite JSON values are rejected",
                     lambda: export_cli.load_profile(nonfinite), "non-finite")

        profile = base_profile(output_a)
        config = export_cli.validate_profile(profile, storage_root=bank)
        check("valid profile normalizes complete fixed-root config",
              config["selected"] == ["AAA", "BBB"]
              and config["present"] == ["AAA", "BBB"]
              and config["absent"] == []
              and config["spec"]["columns"]
              == export_designer.DEFAULT_COLUMNS)
        all_config = export_cli.validate_profile(
            base_profile(output_a, "ALL"), storage_root=bank)
        check("ALL resolves deterministic stored tickers once",
              all_config["selected"] == ["AAA", "BBB"]
              and all_config["present"] == ["AAA", "BBB"])

        overridden = export_cli.apply_overrides(
            profile, destination=output_b, tickers="BBB",
            file_format="tsv", workers=1)
        override_config = export_cli.validate_profile(
            overridden, storage_root=bank)
        check("command flags override profile fields",
              override_config["destination"] == output_b.resolve()
              and override_config["selected"] == ["BBB"]
              and override_config["format"] == "tsv"
              and override_config["workers"] == 1)

        cases = []
        bad = dict(profile, surprise=True)
        cases.append(("unknown fields fail closed", bad, "unknown profile"))
        bad = dict(profile); bad.pop("format")
        cases.append(("missing required field is rejected", bad, "missing profile"))
        cases.append(("bool profile version is rejected",
                      dict(profile, version=True), "version"))
        cases.append(("relative destination is rejected",
                      dict(profile, destination="relative"), "absolute"))
        cases.append(("missing destination directory is rejected",
                      dict(profile, destination=str(project / "No Such")),
                      "existing directory"))
        cases.append(("destination inside bank is rejected",
                      dict(profile, destination=str(bank)), "inside"))
        cases.append(("unknown format is rejected",
                      dict(profile, format="xlsx"), "format"))
        cases.append(("session-qualified interval is rejected",
                      dict(profile, interval="1m-pre"), "base trade"))
        cases.append(("kind interval is rejected",
                      dict(profile, interval="1d-hvol"), "base trade"))
        cases.append(("empty sessions are rejected",
                      dict(profile, sessions=[]), "sessions"))
        cases.append(("duplicate sessions are rejected",
                      dict(profile, sessions=["rth", "RTH"]), "duplicate session"))
        cases.append(("unknown session is rejected",
                      dict(profile, sessions=["overnight"]), "sessions"))
        cases.append(("malformed date range is rejected",
                      dict(profile, range={"start": "2026-1-01",
                                           "end": "2026-01-31"}), "YYYY-MM-DD"))
        cases.append(("reversed date range is rejected",
                      dict(profile, range={"start": "2026-02-01",
                                           "end": "2026-01-31"}), "before"))
        cases.append(("extra range field is rejected",
                      dict(profile, range={"start": "2026-01-01",
                                           "end": "2026-01-31", "x": 1}),
                      "exactly"))
        cases.append(("zero workers is rejected",
                      dict(profile, workers=0), "workers"))
        cases.append(("nine workers is rejected",
                      dict(profile, workers=9), "workers"))
        cases.append(("bool workers is rejected",
                      dict(profile, workers=True), "workers"))
        cases.append(("empty ticker list is rejected",
                      dict(profile, tickers=[]), "non-empty"))
        cases.append(("duplicate ticker is rejected",
                      dict(profile, tickers=["AAA", "aaa"]), "duplicate ticker"))
        cases.append(("invalid ticker is rejected",
                      dict(profile, tickers=["../AAA"]), "invalid ticker"))
        cases.append(("all-absent selection is rejected",
                      dict(profile, tickers=["MISS"]), "none"))
        for name, value, message in cases:
            expect_error(name, lambda value=value: export_cli.validate_profile(
                value, storage_root=bank), message)

        mixed = export_cli.validate_profile(
            dict(profile, tickers=["AAA", "MISS"]), storage_root=bank)
        check("present and absent selection stays ordered and explicit",
              mixed["selected"] == ["AAA", "MISS"]
              and mixed["present"] == ["AAA"]
              and mixed["absent"] == ["MISS"])

        estimate = {"bytes": 3 * 1024 ** 3}
        expect_error("disk hard floor aborts before export",
                     lambda: export_cli.disk_preflight(
                         output_a, estimate,
                         disk_usage=lambda _p: SimpleNamespace(
                             free=export_cli.DISK_FLOOR_BYTES - 1)),
                     "at least 1 GiB")
        soft = export_cli.disk_preflight(
            output_a, estimate,
            disk_usage=lambda _p: SimpleNamespace(free=2 * 1024 ** 3))
        check("disk estimate shortfall warns but does not abort",
              bool(soft["warning"]) and soft["free_bytes"] == 2 * 1024 ** 3)
        unknown_disk = export_cli.disk_preflight(
            output_a, {"bytes": 1},
            disk_usage=lambda _p: (_ for _ in ()).throw(OSError("fixture")))
        check("unavailable disk measurement is an explicit warning",
              unknown_disk["free_bytes"] is None
              and "could not be measured" in unknown_disk["warning"])

        busy = export_cli.prepare_profile(
            profile, storage_root=bank, disk_usage=ample_disk,
            gate_probe=lambda: {"available": False})
        check("busy market-data gate warns and does not block",
              any("market-data operation is active" in warning
                  for warning in busy["warnings"]))
        unknown_gate = export_cli.prepare_profile(
            profile, storage_root=bank, disk_usage=ample_disk,
            gate_probe=lambda: (_ for _ in ()).throw(RuntimeError("fixture")))
        check("gate probe failure is explicit and non-blocking",
              any("could not be determined" in warning
                  for warning in unknown_gate["warnings"]))

        old_gate_path = operation_gate.LOCK_PATH
        missing_gate_path = logs / "never-created.lock"
        operation_gate.LOCK_PATH = missing_gate_path
        try:
            gate = export_cli.operation_status()
            check("read-only gate probe does not create a missing lock file",
                  gate["available"] is True and not missing_gate_path.exists())
            lease = operation_gate.acquire("fetch", path=missing_gate_path)
            try:
                gate = export_cli.operation_status()
                check("read-only gate probe detects an active operation",
                      gate["available"] is False)
            finally:
                lease.release()
        finally:
            operation_gate.LOCK_PATH = old_gate_path

        dry_profile = project / "dry-profile.json"
        write_profile(dry_profile, profile)
        before_dry = tree_snapshot(project)
        out, err = io.StringIO(), io.StringIO()
        dry_rc = export_cli.cli(
            ["--profile", str(dry_profile), "--dry-run"],
            storage_root=bank, stdout=out, stderr=err,
            disk_usage=ample_disk, gate_probe=available_gate)
        after_dry = tree_snapshot(project)
        check("--dry-run exits 0 and reports no writes",
              dry_rc == export_cli.EXIT_COMPLETE
              and "DRY RUN" in out.getvalue() and not err.getvalue())
        check("--dry-run leaves the complete fixture tree unchanged",
              before_dry == after_dry)

        override_out, override_err = io.StringIO(), io.StringIO()
        override_rc = export_cli.cli(
            ["--profile", str(dry_profile), "--dest", str(output_b),
             "--tickers", "BBB", "--format", "tsv", "--workers", "1",
             "--dry-run"], storage_root=bank, stdout=override_out,
            stderr=override_err, disk_usage=ample_disk,
            gate_probe=available_gate)
        check("CLI overrides win in rendered dry-run",
              override_rc == 0 and str(output_b.resolve()) in override_out.getvalue()
              and "Selection: 1 selected" in override_out.getvalue()
              and "Format: tsv" in override_out.getvalue()
              and "Workers: 1" in override_out.getvalue())

        init_path = project / "cli-init.json"
        init_out, init_err = io.StringIO(), io.StringIO()
        init_rc = export_cli.cli(
            ["--profile", str(init_path), "--init"], stdout=init_out,
            stderr=init_err)
        repeat_rc = export_cli.cli(
            ["--profile", str(init_path), "--init"],
            stdout=io.StringIO(), stderr=init_err)
        check("CLI --init succeeds once and refuses overwrite with exit 2",
              init_rc == 0 and repeat_rc == 2
              and export_cli.load_profile(init_path)
              == export_cli.profile_template())

        bad_out, bad_err = io.StringIO(), io.StringIO()
        bad_rc = export_cli.cli(
            ["--profile", str(malformed), "--dry-run"],
            storage_root=bank, stdout=bad_out, stderr=bad_err,
            disk_usage=ample_disk, gate_probe=available_gate)
        check("invalid profile exits 2 without traceback",
              bad_rc == export_cli.EXIT_PREFLIGHT
              and "ERROR:" in bad_err.getvalue()
              and "Traceback" not in bad_err.getvalue())

        def interrupted_disk(_destination):
            raise KeyboardInterrupt

        preflight_err = io.StringIO()
        preflight_cancel_rc = export_cli.cli(
            ["--profile", str(dry_profile)], storage_root=bank,
            stdout=io.StringIO(), stderr=preflight_err,
            disk_usage=interrupted_disk, gate_probe=available_gate)
        check("preflight Ctrl+C exits 4 without traceback",
              preflight_cancel_rc == export_cli.EXIT_CANCELLED
              and "before export started" in preflight_err.getvalue()
              and "Traceback" not in preflight_err.getvalue())

        check("terminal exit-code mapping pins 0/3/4",
              export_cli.result_exit_code(result_stub()) == 0
              and export_cli.result_exit_code(
                  result_stub("PARTIAL")) == 3
              and export_cli.result_exit_code(
                  result_stub(note_error="fixture")) == 3
              and export_cli.result_exit_code(
                  result_stub("CANCELLED", cancelled=1)) == 4)

        cancel_seen = threading.Event()

        def cancel_runner(*_args, cancel=None, **_kwargs):
            if cancel is None or not cancel.wait(2.0):
                raise AssertionError("cancel event was not delivered")
            cancel_seen.set()
            return result_stub("CANCELLED", cancelled=2)

        class InterruptOnce:
            def __init__(self):
                self.used = False

            def __call__(self, event, timeout):
                if not self.used:
                    self.used = True
                    raise KeyboardInterrupt
                return event.wait(timeout)

        cancel_out, cancel_err = io.StringIO(), io.StringIO()
        cancel_rc = export_cli.cli(
            ["--profile", str(dry_profile)], storage_root=bank,
            stdout=cancel_out, stderr=cancel_err, disk_usage=ample_disk,
            gate_probe=available_gate, batch_runner=cancel_runner,
            wait=InterruptOnce())
        check("injected Ctrl+C sets cancel, waits, and exits 4",
              cancel_rc == export_cli.EXIT_CANCELLED and cancel_seen.is_set()
              and "safe boundary" in cancel_err.getvalue()
              and "Result: CANCELLED" in cancel_out.getvalue())

        boundary = {}

        def boundary_interrupt_runner(*_args, cancel=None, **_kwargs):
            boundary["cancel"] = cancel
            raise KeyboardInterrupt

        boundary_err = io.StringIO()
        boundary_rc = export_cli.cli(
            ["--profile", str(dry_profile)], storage_root=bank,
            stdout=io.StringIO(), stderr=boundary_err,
            disk_usage=ample_disk, gate_probe=available_gate,
            batch_runner=boundary_interrupt_runner)
        check("boundary Ctrl+C sets cancel and exits 4 without traceback",
              boundary_rc == export_cli.EXIT_CANCELLED
              and boundary["cancel"].is_set()
              and "safe boundary" in boundary_err.getvalue()
              and "Traceback" not in boundary_err.getvalue())

        def partial_runner(*_args, **_kwargs):
            return result_stub("PARTIAL")

        partial_rc = export_cli.cli(
            ["--profile", str(dry_profile)], storage_root=bank,
            stdout=io.StringIO(), stderr=io.StringIO(),
            disk_usage=ample_disk, gate_probe=available_gate,
            batch_runner=partial_runner)
        check("completed partial batch exits 3", partial_rc == 3)

        def failed_runner(*_args, **_kwargs):
            raise RuntimeError("injected post-start failure")

        failed_err = io.StringIO()
        failed_rc = export_cli.cli(
            ["--profile", str(dry_profile)], storage_root=bank,
            stdout=io.StringIO(), stderr=failed_err,
            disk_usage=ample_disk, gate_probe=available_gate,
            batch_runner=failed_runner)
        check("post-start exception exits 3 without traceback",
              failed_rc == export_cli.EXIT_PARTIAL
              and "injected post-start failure" in failed_err.getvalue()
              and "Traceback" not in failed_err.getvalue())

        dated_profile = project / "dated-profile.json"
        dated_value = base_profile(output_b, ["AAA"])
        dated_value["range"] = {
            "start": "2026-01-05", "end": "2026-01-05"}
        write_profile(dated_profile, dated_value)
        dated_rc = export_cli.cli(
            ["--profile", str(dated_profile)], storage_root=bank,
            stdout=io.StringIO(), stderr=io.StringIO(),
            disk_usage=ample_disk, gate_probe=available_gate, now=NOW)
        dated_folders = list(output_b.glob("Export *"))
        dated_file = (dated_folders[0] / "AAA_1m.csv"
                      if len(dated_folders) == 1 else output_b / "missing")
        check("fixed-date profile clips a real single-ticker export",
              dated_rc == 0 and dated_file.is_file()
              and len(dated_file.read_text(encoding="utf-8").splitlines()) == 2
              and (dated_folders[0] / "health_report.json").is_file())

        actual_profile = project / "actual-profile.json"
        write_profile(actual_profile, base_profile(output_a))
        bank_before = tree_snapshot(bank)
        actual_out, actual_err = io.StringIO(), io.StringIO()
        actual_rc = export_cli.cli(
            ["--profile", str(actual_profile)], storage_root=bank,
            stdout=actual_out, stderr=actual_err, disk_usage=ample_disk,
            gate_probe=available_gate, quality_kwargs=quality, now=NOW)
        check("real shared-orchestrator CLI run exits complete",
              actual_rc == 0 and not actual_err.getvalue()
              and "Quality note:" in actual_out.getvalue())
        actual_folders = list(output_a.glob("Export *"))
        check("CLI run creates Data plus sibling quality and health reports",
              len(actual_folders) == 1
              and len(list((actual_folders[0] / "Data").glob("*.csv"))) == 2
              and (actual_folders[0] / "health_report.json").is_file()
              and len(list(actual_folders[0].glob(
                  "EXPORT_DATA_QUALITY_*.txt"))) == 1)
        check("CLI reports the outer delivery bundle, not its Data child",
              f"Folder: {actual_folders[0]}" in actual_out.getvalue())

        direct_config = export_cli.prepare_profile(
            base_profile(output_c), storage_root=bank,
            disk_usage=ample_disk, gate_probe=available_gate)
        direct = export_batch.run_designer_batch(
            bank, direct_config["selected"], direct_config["present"],
            output_c, direct_config["spec"], workers=2, now=NOW,
            quality_kwargs=quality)
        direct_folder = Path(direct["bundle_folder"])
        direct_data = Path(direct["folder"])
        check("CLI and GUI-engine paths produce byte-identical data files",
              all((actual_folders[0] / "Data" / f"{ticker}_1m.csv").read_bytes()
                  == (direct_data / f"{ticker}_1m.csv").read_bytes()
                  for ticker in ("AAA", "BBB")))
        check("CLI and GUI-engine health reports are byte-identical",
              (actual_folders[0] / "health_report.json").read_bytes()
              == (direct_folder / "health_report.json").read_bytes())
        check("complete CLI and direct exports leave bank tree unchanged",
              tree_snapshot(bank) == bank_before)

        source = Path(export_cli.__file__).read_text(encoding="utf-8")
        tree = ast.parse(source)
        imports = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imports.update(alias.name.split(".")[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imports.add(node.module.split(".")[0])
        check("CLI import graph is tkinter/network/process free",
              "tkinter" not in imports
              and imports.isdisjoint({
                  "ib_async", "requests", "urllib", "socket", "subprocess"}))
        check("CLI has no caller-selected bank-root argument",
              "--root" not in source and "run_designer_batch" in source)

        root = Path(export_cli.__file__).parent.parent
        batch_text = (root / "Export Bank.bat").read_text(encoding="ascii")
        ignore_text = (root / ".gitignore").read_text(encoding="utf-8")
        check("launcher prefers project Python and preserves exit code",
              "pythoncore-3.14-64" in batch_text
              and "EXPORT_RC=%ERRORLEVEL%" in batch_text
              and "pause" in batch_text.lower())
        check("user profile is ignored while example stays tracked",
              "/export_profile.json" in ignore_text)
    finally:
        shutil.rmtree(project, ignore_errors=True)


if __name__ == "__main__":
    run()
    if FAILURES:
        print(f"FAILED: {len(FAILURES)}/{COUNT[0]} checks: "
              + ", ".join(FAILURES))
        raise SystemExit(1)
    print(f"ALL PASS ({COUNT[0]}/{COUNT[0]}; temp bank; no network)")
