"""One-command, profile-driven export over the shared batch orchestrator."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import shutil
import sys
import threading
from pathlib import Path

import export_batch
import export_designer
import operation_gate
import stock_storage as storage


PROJECT_ROOT = Path(__file__).resolve().parent.parent
STORAGE_ROOT = storage.storage_root(PROJECT_ROOT)
DEFAULT_PROFILE = PROJECT_ROOT / "export_profile.json"

PROFILE_VERSION = 1
PROFILE_MAX_BYTES = 64 * 1024
MAX_TICKERS = 1_000
MAX_WORKERS = 8
DISK_FLOOR_BYTES = 1 * 1024 ** 3
FORMATS = ("csv", "tsv", "parquet")
SESSION_TOKENS = ("rth", "pre", "post")
PROFILE_KEYS = {
    "version", "destination", "format", "interval", "sessions", "range",
    "tickers", "workers",
}
PROFILE_REQUIRED = PROFILE_KEYS - {"workers"}
_DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}\Z")

EXIT_COMPLETE = 0
EXIT_PREFLIGHT = 2
EXIT_PARTIAL = 3
EXIT_CANCELLED = 4


class ExportCommandError(RuntimeError):
    """The profile or preflight cannot safely start an export."""


def _bounded(value, limit=400):
    text = " ".join(str(value or "").split())
    if len(text) <= limit:
        return text
    return text[:max(0, limit - 3)].rstrip() + "..."


def profile_template():
    """Return the valid JSON template used by ``--init`` and the example."""
    return {
        "version": PROFILE_VERSION,
        "destination": "",
        "format": "csv",
        "interval": "1m",
        "sessions": ["rth"],
        "range": "all",
        "tickers": "ALL",
        "workers": 4,
    }


def _template_bytes():
    return (json.dumps(profile_template(), indent=2, ensure_ascii=True)
            + "\n").encode("utf-8")


def write_profile_template(path):
    """Exclusively create a valid profile template; never overwrite."""
    target = Path(path)
    if not target.parent.is_dir():
        raise ExportCommandError("profile parent is not an existing directory")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    fd = None
    created = False
    try:
        fd = os.open(target, flags, 0o600)
        created = True
        with os.fdopen(fd, "wb") as handle:
            fd = None
            handle.write(_template_bytes())
            handle.flush()
            os.fsync(handle.fileno())
    except FileExistsError as exc:
        raise ExportCommandError(
            f"profile already exists; refusing to overwrite: {target}") from exc
    except OSError as exc:
        if fd is not None:
            try:
                os.close(fd)
            except OSError:
                pass
        if created:
            try:
                target.unlink()
            except OSError:
                pass
        raise ExportCommandError(
            f"could not create profile: {type(exc).__name__}: {exc}") from exc
    return target


def _no_duplicate_pairs(pairs):
    out = {}
    for key, value in pairs:
        if key in out:
            raise ExportCommandError(f"duplicate JSON key: {key!r}")
        out[key] = value
    return out


def _reject_constant(value):
    raise ExportCommandError(f"non-finite JSON value is not allowed: {value}")


def load_profile(path):
    """Strictly load one small, stable UTF-8 JSON profile."""
    target = Path(path)
    try:
        before = target.stat()
    except FileNotFoundError as exc:
        raise ExportCommandError(
            f"profile not found: {target}; run with --init first") from exc
    except OSError as exc:
        raise ExportCommandError(
            f"cannot inspect profile: {type(exc).__name__}: {exc}") from exc
    if not target.is_file():
        raise ExportCommandError("profile path is not a file")
    if before.st_size > PROFILE_MAX_BYTES:
        raise ExportCommandError("profile is larger than 64 KiB")
    try:
        raw = target.read_bytes()
        after = target.stat()
    except OSError as exc:
        raise ExportCommandError(
            f"cannot read profile: {type(exc).__name__}: {exc}") from exc
    if (before.st_size, before.st_mtime_ns) != (
            after.st_size, after.st_mtime_ns):
        raise ExportCommandError("profile changed while it was being read")
    try:
        text = raw.decode("utf-8")
        value = json.loads(
            text, object_pairs_hook=_no_duplicate_pairs,
            parse_constant=_reject_constant)
    except ExportCommandError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ExportCommandError(
            f"profile is not valid UTF-8 JSON: {_bounded(exc)}") from exc
    if not isinstance(value, dict):
        raise ExportCommandError("profile root must be a JSON object")
    return value


def parse_ticker_override(value):
    text = str(value or "").strip()
    if text.upper() == "ALL":
        return "ALL"
    tickers = [item.strip() for item in text.split(",")]
    if not tickers or any(not item for item in tickers):
        raise ExportCommandError(
            "--tickers must be ALL or a comma-separated ticker list")
    return tickers


def apply_overrides(profile, *, destination=None, tickers=None,
                    file_format=None, workers=None):
    if not isinstance(profile, dict):
        raise ExportCommandError("profile root must be a JSON object")
    out = dict(profile)
    if destination is not None:
        out["destination"] = str(destination)
    if tickers is not None:
        out["tickers"] = parse_ticker_override(tickers)
    if file_format is not None:
        out["format"] = str(file_format)
    if workers is not None:
        out["workers"] = workers
    return out


def _inside(path, root):
    child = Path(path).resolve()
    parent = Path(root).resolve()
    return child == parent or parent in child.parents


def _profile_date(value, label):
    if not isinstance(value, str) or not _DATE_RE.fullmatch(value):
        raise ExportCommandError(f"range.{label} must be YYYY-MM-DD")
    try:
        return dt.date.fromisoformat(value)
    except ValueError as exc:
        raise ExportCommandError(f"range.{label} is not a real date") from exc


def _profile_tickers(value, root):
    if isinstance(value, str):
        if value.upper() != "ALL":
            raise ExportCommandError("tickers must be ALL or a JSON list")
        selected = export_designer.stored_tickers(root)
        if not selected:
            raise ExportCommandError("the bank contains no exportable tickers")
        return selected, True
    if not isinstance(value, list) or not value:
        raise ExportCommandError("tickers must be ALL or a non-empty JSON list")
    if len(value) > MAX_TICKERS:
        raise ExportCommandError("ticker list exceeds 1,000 entries")
    selected, seen = [], set()
    for raw in value:
        if not isinstance(raw, str):
            raise ExportCommandError("each ticker must be a string")
        if ".." in raw.strip():
            raise ExportCommandError(f"invalid ticker: {raw!r}")
        try:
            ticker = storage.canonical_ticker(raw)
        except storage.StorageError as exc:
            raise ExportCommandError(f"invalid ticker: {raw!r}") from exc
        if ticker in seen:
            raise ExportCommandError(f"duplicate ticker: {ticker}")
        seen.add(ticker)
        selected.append(ticker)
    return selected, False


def validate_profile(profile, *, storage_root=STORAGE_ROOT):
    """Validate and normalize a raw profile without writing anything."""
    if not isinstance(profile, dict):
        raise ExportCommandError("profile root must be a JSON object")
    keys = set(profile)
    unknown = sorted(keys - PROFILE_KEYS)
    missing = sorted(PROFILE_REQUIRED - keys)
    if unknown:
        raise ExportCommandError(
            "unknown profile field(s): " + ", ".join(unknown))
    if missing:
        raise ExportCommandError(
            "missing profile field(s): " + ", ".join(missing))
    version = profile.get("version")
    if isinstance(version, bool) or version != PROFILE_VERSION:
        raise ExportCommandError(
            f"profile version must be {PROFILE_VERSION}")

    root = Path(storage_root).resolve()
    if not root.is_dir():
        raise ExportCommandError(f"storage bank not found: {root}")
    destination_raw = profile.get("destination")
    if not isinstance(destination_raw, str) or not destination_raw.strip():
        raise ExportCommandError(
            "destination is empty; edit the profile or pass --dest")
    destination_path = Path(destination_raw.strip())
    if not destination_path.is_absolute():
        raise ExportCommandError("destination must be an absolute path")
    try:
        destination = destination_path.resolve()
    except OSError as exc:
        raise ExportCommandError(
            f"cannot resolve destination: {type(exc).__name__}: {exc}") from exc
    if not destination.is_dir():
        raise ExportCommandError("destination is not an existing directory")
    if _inside(destination, root):
        raise ExportCommandError("destination cannot be inside the storage bank")

    file_format = profile.get("format")
    if not isinstance(file_format, str) or file_format not in FORMATS:
        raise ExportCommandError(
            "format must be one of: " + ", ".join(FORMATS))
    interval = profile.get("interval")
    if (not isinstance(interval, str)
            or storage.INTERVAL_RE.fullmatch(interval) is None
            or storage.base_interval(interval) != interval
            or storage.kind_of(interval)):
        raise ExportCommandError(
            "interval must be a base trade interval such as 1m or 1d")

    raw_sessions = profile.get("sessions")
    if not isinstance(raw_sessions, list) or not raw_sessions:
        raise ExportCommandError("sessions must be a non-empty JSON list")
    sessions, seen_sessions = [], set()
    for raw in raw_sessions:
        if not isinstance(raw, str) or raw.lower() not in SESSION_TOKENS:
            raise ExportCommandError(
                "sessions may contain only rth, pre, and post")
        session = raw.lower()
        if session in seen_sessions:
            raise ExportCommandError(f"duplicate session: {session}")
        seen_sessions.add(session)
        sessions.append(session)

    raw_range = profile.get("range")
    if isinstance(raw_range, str) and raw_range.lower() == "all":
        range_mode = "all"
        start_date = end_date = None
    elif isinstance(raw_range, dict):
        if set(raw_range) != {"start", "end"}:
            raise ExportCommandError(
                "range object must contain exactly start and end")
        start_date = _profile_date(raw_range["start"], "start")
        end_date = _profile_date(raw_range["end"], "end")
        if end_date < start_date:
            raise ExportCommandError("range.end is before range.start")
        range_mode = "dates"
    else:
        raise ExportCommandError(
            "range must be ALL or an object with start/end dates")

    workers = profile.get("workers")
    if workers is not None and (
            isinstance(workers, bool) or not isinstance(workers, int)
            or workers < 1 or workers > MAX_WORKERS):
        raise ExportCommandError("workers must be an integer from 1 through 8")

    selected, all_tickers = _profile_tickers(profile.get("tickers"), root)
    if len(selected) > MAX_TICKERS:
        raise ExportCommandError("bank selection exceeds 1,000 tickers")
    if all_tickers:
        present, absent = list(selected), []
    else:
        present, absent = export_designer.missing_from_bank(root, selected)
    if not present:
        raise ExportCommandError("none of the selected tickers is in the bank")

    spec = export_designer.default_spec()
    spec.update({
        "base_interval": interval,
        "sessions": list(sessions),
        "file_type": file_format,
        "delimiter": "\t" if file_format == "tsv" else ",",
        "range_preset": "furthest",
        "start_date": start_date,
        "end_date": end_date,
    })
    return {
        "version": PROFILE_VERSION,
        "destination": destination,
        "format": file_format,
        "interval": interval,
        "sessions": sessions,
        "range_mode": range_mode,
        "start_date": start_date,
        "end_date": end_date,
        "selected": selected,
        "present": present,
        "absent": absent,
        "all_tickers": all_tickers,
        "workers": workers,
        "spec": spec,
    }


def estimate_profile(config, *, storage_root=STORAGE_ROOT):
    preset = "furthest" if config["range_mode"] == "all" else None
    return export_designer.estimate(
        storage_root, config["present"], config["interval"],
        config["sessions"], config["start_date"], config["end_date"],
        config["spec"], preset=preset)


def disk_preflight(destination, estimate, *, disk_usage=shutil.disk_usage):
    """Return disk evidence; abort only below the hard 1 GiB floor."""
    try:
        free = int(disk_usage(destination).free)
    except OSError as exc:
        return {
            "free_bytes": None,
            "estimated_bytes": int(estimate.get("bytes", 0) or 0),
            "warning": ("disk space could not be measured: "
                        f"{type(exc).__name__}"),
        }
    if free < DISK_FLOOR_BYTES:
        raise ExportCommandError(
            f"disk preflight: only {free / (1024 ** 3):.2f} GiB free; "
            "at least 1 GiB is required")
    expected = int(estimate.get("bytes", 0) or 0)
    warning = None
    if free < expected:
        warning = (f"estimated output is {expected / (1024 ** 3):.2f} GiB "
                   f"but only {free / (1024 ** 3):.2f} GiB is free")
    return {
        "free_bytes": free,
        "estimated_bytes": expected,
        "warning": warning,
    }


def operation_status():
    """Probe an existing gate without creating its lock file on dry-run."""
    path = Path(operation_gate.LOCK_PATH)
    try:
        if not path.is_file() or path.stat().st_size == 0:
            return {"available": True, "path": str(path)}
        return operation_gate.status(path=path)
    except OSError as exc:
        return {
            "available": None,
            "path": str(path),
            "error": type(exc).__name__,
        }


def prepare_profile(profile, *, storage_root=STORAGE_ROOT,
                    disk_usage=shutil.disk_usage, gate_probe=operation_status):
    config = validate_profile(profile, storage_root=storage_root)
    estimate = estimate_profile(config, storage_root=storage_root)
    disk = disk_preflight(
        config["destination"], estimate, disk_usage=disk_usage)
    try:
        gate = dict(gate_probe())
    except Exception as exc:  # noqa: BLE001 - availability is advisory
        gate = {"available": None, "error": type(exc).__name__}
    warnings = []
    if config["absent"]:
        warnings.append(
            f"{len(config['absent'])} selected ticker(s) are not in the bank")
    if disk.get("warning"):
        warnings.append(disk["warning"])
    if gate.get("available") is False:
        warnings.append(
            "another market-data operation is active; continuing with "
            "read-only SHA-gated export reads")
    elif gate.get("available") is None:
        warnings.append(
            "market-data operation status could not be determined")
    return {**config, "estimate": estimate, "disk": disk,
            "gate": gate, "warnings": warnings}


def _format_bytes(value):
    count = int(value or 0)
    units = ("B", "KiB", "MiB", "GiB", "TiB")
    amount = float(count)
    for unit in units:
        if amount < 1024.0 or unit == units[-1]:
            return f"{amount:.2f} {unit}" if unit != "B" else f"{count} B"
        amount /= 1024.0
    return f"{count} B"


def render_preflight(config):
    estimate = config["estimate"]
    workers = config["workers"] if config["workers"] is not None else "auto"
    lines = [
        f"Destination: {config['destination']}",
        (f"Selection: {len(config['selected'])} selected, "
         f"{len(config['present'])} present, {len(config['absent'])} absent"),
        (f"Format: {config['format']}  Interval: {config['interval']}  "
         f"Sessions: {'+'.join(config['sessions'])}  Workers: {workers}"),
        (f"Estimate: {estimate.get('files', 0):,} file(s), "
         f"{estimate.get('rows', 0):,} row(s), "
         f"{_format_bytes(estimate.get('bytes', 0))}"),
    ]
    lines.extend(f"WARNING: {warning}" for warning in config["warnings"])
    return "\n".join(lines)


def run_profile(config, *, storage_root=STORAGE_ROOT,
                batch_runner=export_batch.run_designer_batch,
                progress=None, cancel=None, now=None, quality_kwargs=None):
    return batch_runner(
        storage_root, config["selected"], config["present"],
        config["destination"], config["spec"], workers=config["workers"],
        progress=progress, cancel=cancel, now=now,
        quality_kwargs=quality_kwargs)


def run_interruptible(config, *, storage_root=STORAGE_ROOT,
                      batch_runner=export_batch.run_designer_batch,
                      progress=None, cancel=None, now=None,
                      quality_kwargs=None, wait=None, on_interrupt=None):
    """Run in a worker so Ctrl+C can request a clean orchestrator cancel."""
    cancel_event = cancel if cancel is not None else threading.Event()
    done = threading.Event()
    holder = {}

    def worker():
        try:
            holder["result"] = run_profile(
                config, storage_root=storage_root, batch_runner=batch_runner,
                progress=progress, cancel=cancel_event, now=now,
                quality_kwargs=quality_kwargs)
        except BaseException as exc:  # propagate on the main thread
            holder["error"] = exc
        finally:
            done.set()

    thread = threading.Thread(
        target=worker, name="export-command", daemon=False)
    thread.start()
    interrupted = False
    waiter = wait or (lambda event, timeout: event.wait(timeout))

    def request_cancel():
        nonlocal interrupted
        cancel_event.set()
        if interrupted:
            return
        interrupted = True
        if on_interrupt is not None:
            on_interrupt()

    while True:
        try:
            if done.is_set():
                break
            waiter(done, 0.1)
        except KeyboardInterrupt:
            request_cancel()
    while True:
        try:
            if not thread.is_alive():
                break
            thread.join(0.1)
        except KeyboardInterrupt:
            request_cancel()
    if "error" in holder:
        raise holder["error"]
    if "result" not in holder:
        raise ExportCommandError("export worker returned no result")
    return holder["result"], interrupted


def result_exit_code(result, *, interrupted=False):
    summary = result.get("summary") or {}
    if (interrupted or result.get("state") == "CANCELLED"
            or summary.get("cancelled", 0)):
        return EXIT_CANCELLED
    if result.get("state") != "COMPLETE" or result.get("note_error"):
        return EXIT_PARTIAL
    return EXIT_COMPLETE


def render_result(result):
    summary = result.get("summary") or {}
    lines = [
        f"Result: {result.get('state', 'UNKNOWN')}",
        f"Folder: {result.get('bundle_folder') or result.get('folder', '')}",
        (f"Files: {summary.get('files_written', 0):,}  "
         f"Rows: {summary.get('rows', 0):,}"),
    ]
    if result.get("note_path"):
        lines.append(f"Quality note: {result['note_path']}")
    elif result.get("note_error"):
        lines.append(f"Quality note FAILED: {result['note_error']}")
    else:
        lines.append("Quality note: not required for this run")
    lines.append(f"Elapsed: {float(result.get('elapsed_seconds', 0.0)):.2f}s")
    failures = [
        row.get("ticker", "?") for row in (result.get("results") or [])
        if row.get("error") or row.get("not_in_bank")
    ]
    if failures:
        shown = ", ".join(failures[:20])
        if len(failures) > 20:
            shown += f", ... (+{len(failures) - 20})"
        lines.append(f"Failed/absent: {shown}")
    return "\n".join(lines)


def build_parser():
    parser = argparse.ArgumentParser(
        description="Export the fixed local bank from a saved JSON profile.")
    parser.add_argument("--profile", default=str(DEFAULT_PROFILE),
                        help="profile path (default: project export_profile.json)")
    parser.add_argument("--dest", help="override destination directory")
    parser.add_argument("--tickers",
                        help="override with ALL or comma-separated tickers")
    parser.add_argument("--format", dest="file_format", choices=FORMATS,
                        help="override output format")
    parser.add_argument("--workers", type=int,
                        help="override worker count (1-8)")
    parser.add_argument("--dry-run", action="store_true",
                        help="validate and estimate; write nothing")
    parser.add_argument("--init", action="store_true",
                        help="create a valid profile template without overwrite")
    return parser


def cli(argv=None, *, storage_root=STORAGE_ROOT, stdout=None, stderr=None,
        disk_usage=shutil.disk_usage, gate_probe=operation_status,
        batch_runner=export_batch.run_designer_batch, quality_kwargs=None,
        now=None, wait=None):
    out = stdout or sys.stdout
    err = stderr or sys.stderr
    run_started = False
    cancel_event = threading.Event()
    try:
        args = build_parser().parse_args(argv)
        profile_path = Path(args.profile)
        if args.init:
            if (args.dest is not None or args.tickers is not None
                    or args.file_format is not None or args.workers is not None
                    or args.dry_run):
                raise ExportCommandError(
                    "--init may be combined only with --profile")
            created = write_profile_template(profile_path)
            print(f"Created profile: {created}", file=out)
            print("Edit destination, then run this command again.", file=out)
            return EXIT_COMPLETE

        raw = load_profile(profile_path)
        raw = apply_overrides(
            raw, destination=args.dest, tickers=args.tickers,
            file_format=args.file_format, workers=args.workers)
        config = prepare_profile(
            raw, storage_root=storage_root, disk_usage=disk_usage,
            gate_probe=gate_probe)
        print(render_preflight(config), file=out)
        if args.dry_run:
            print("DRY RUN: no folders, data files, or notes were written.",
                  file=out)
            return EXIT_COMPLETE

        def progress(event):
            print(export_batch.progress_text(event), file=out, flush=True)

        def interrupted():
            print("Cancellation requested; waiting for a safe boundary...",
                  file=err, flush=True)

        run_started = True
        result, was_interrupted = run_interruptible(
            config, storage_root=storage_root, batch_runner=batch_runner,
            progress=progress, cancel=cancel_event, now=now,
            quality_kwargs=quality_kwargs, wait=wait,
            on_interrupt=interrupted)
        print(render_result(result), file=out)
        return result_exit_code(result, interrupted=was_interrupted)
    except KeyboardInterrupt:
        cancel_event.set()
        message = ("Cancellation requested; export is stopping at a safe boundary."
                   if run_started else
                   "Cancellation requested before export started.")
        print(message, file=err, flush=True)
        return EXIT_CANCELLED
    except (ExportCommandError, export_batch.BatchExportError,
            storage.StorageError, OSError, ValueError) as exc:
        print(f"ERROR: {_bounded(exc)}", file=err)
        return EXIT_PARTIAL if run_started else EXIT_PREFLIGHT
    except Exception as exc:  # noqa: BLE001 - CLI never emits a traceback
        print(f"ERROR: {type(exc).__name__}: {_bounded(exc)}", file=err)
        return EXIT_PARTIAL if run_started else EXIT_PREFLIGHT


def main():
    raise SystemExit(cli())


if __name__ == "__main__":
    main()


__all__ = [
    "DISK_FLOOR_BYTES",
    "EXIT_CANCELLED",
    "EXIT_COMPLETE",
    "EXIT_PARTIAL",
    "EXIT_PREFLIGHT",
    "ExportCommandError",
    "apply_overrides",
    "cli",
    "disk_preflight",
    "estimate_profile",
    "load_profile",
    "operation_status",
    "prepare_profile",
    "profile_template",
    "render_preflight",
    "render_result",
    "result_exit_code",
    "run_interruptible",
    "run_profile",
    "validate_profile",
    "write_profile_template",
]
