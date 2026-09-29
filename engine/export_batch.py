"""Shared headless orchestration for GUI and command-line batch exports."""

from __future__ import annotations

import datetime as dt
import itertools
import json
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import export_batch_folder as batch_folder
import export_csv
import export_designer
import export_quality
import stock_storage as storage


RESULT_KIND = "export_batch_result"
RESULT_VERSION = 1
MAX_TICKERS = 1_000
MAX_WORKERS = 8
MAX_ERROR = 320
AGGREGATE_EMIT_MIN_S = 0.2
_MONTH_RE = re.compile(r"\d{4}-\d{2}\Z")
_RUN_COUNTER = itertools.count(1)
_UNRESOLVED = object()


class BatchExportError(RuntimeError):
    """The batch cannot safely start or its contract was violated."""


def _bounded(value, limit=MAX_ERROR):
    text = " ".join(str(value or "").split())
    if len(text) <= limit:
        return text
    return text[:max(0, limit - 3)].rstrip() + "..."


def _clock(now=None):
    if now is None:
        value = dt.datetime.now().astimezone()
    elif isinstance(now, dt.datetime):
        value = now
    else:
        raise BatchExportError("injected batch clock must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise BatchExportError("injected batch clock must include a timezone")
    return value


def _inside(path, root):
    path, root = Path(path).resolve(), Path(root).resolve()
    return path == root or root in path.parents


def _cancel_requested(cancel):
    if cancel is None:
        return False
    is_set = getattr(cancel, "is_set", None)
    if callable(is_set):
        return bool(is_set())
    if callable(cancel):
        return bool(cancel())
    raise BatchExportError("cancel must be callable or event-like")


def _canonical_selection(selected, present):
    if not isinstance(selected, (list, tuple)) or not selected:
        raise BatchExportError("selected tickers must be a non-empty list")
    if len(selected) > MAX_TICKERS:
        raise BatchExportError("too many selected tickers")
    names = []
    for value in selected:
        try:
            ticker = storage.canonical_ticker(value)
        except Exception as exc:  # noqa: BLE001 - normalize storage errors
            raise BatchExportError(f"invalid selected ticker: {value!r}") from exc
        if ticker in names:
            raise BatchExportError("selected tickers contain a duplicate")
        names.append(ticker)
    if not isinstance(present, (list, tuple)):
        raise BatchExportError("present tickers must be a list")
    present_names = []
    for value in present:
        try:
            ticker = storage.canonical_ticker(value)
        except Exception as exc:  # noqa: BLE001
            raise BatchExportError(f"invalid present ticker: {value!r}") from exc
        if ticker in present_names or ticker not in names:
            raise BatchExportError("present tickers are duplicated or not selected")
        present_names.append(ticker)
    if not present_names:
        raise BatchExportError("no selected ticker is present in the bank")
    present_set = set(present_names)
    return names, [ticker for ticker in names if ticker in present_set]


def _filename(value, ticker):
    text = str(value or "").strip()
    if not text or len(text) > 240 or Path(text).name != text:
        raise BatchExportError(f"invalid output filename for {ticker}")
    return text


def _target_names(present, filename_for):
    if not callable(filename_for):
        raise BatchExportError("filename_for must be callable")
    names, folded = {}, set()
    for ticker in present:
        name = _filename(filename_for(ticker), ticker)
        key = name.casefold()
        if key in folded:
            raise BatchExportError("two selected tickers resolve to one filename")
        folded.add(key)
        names[ticker] = name
    return names


def _remove_empty_bundle(folder, data_dir):
    """Best-effort setup cleanup without recursive deletion."""
    folder, data_dir = Path(folder), Path(data_dir)
    if data_dir != folder:
        try:
            data_dir.rmdir()
        except OSError:
            pass
    try:
        return batch_folder.remove_created_folder_if_empty(folder)
    except batch_folder.BatchFolderError:
        return False


def _bundle_current_health(root, report_path):
    """Copy the current bank health sidecar byte-for-byte into the bundle."""
    report_path = Path(report_path)
    try:
        report = export_quality.ensure_current_health(root)
        if not isinstance(report, dict):
            raise BatchExportError("health report is not an object")
        source_path = (Path(root) /
                       export_quality.health_report.HEALTH_REPORT_FILE)
        if source_path.is_symlink():
            raise BatchExportError("current health sidecar is linked")
        source = source_path.resolve()
        if not source.is_file():
            raise BatchExportError("current health sidecar is unavailable")
        declared = report.get("report_path")
        if declared and Path(declared).resolve() != source:
            raise BatchExportError("current health sidecar path disagrees")
        payload = source.read_bytes()
        decoded = json.loads(payload.decode("utf-8"))
        canonical = json.dumps(
            report, sort_keys=True, separators=(",", ":"),
            allow_nan=False).encode("utf-8")
        if not isinstance(decoded, dict) or payload != canonical:
            raise BatchExportError("current health sidecar content disagrees")
        if report_path.exists() or report_path.is_symlink():
            raise BatchExportError("health report destination already exists")
        storage._atomic_write_bytes(report_path, payload)
        if report_path.read_bytes() != payload:
            raise BatchExportError("health report copy readback mismatch")
        return report_path
    except BatchExportError:
        raise
    except Exception as exc:  # noqa: BLE001 - normalize bundle setup failures
        raise BatchExportError(
            f"cannot bundle current health report: {type(exc).__name__}") from exc


def _run_id(now):
    return (f"export-{now.strftime('%Y%m%d-%H%M%S')}-{os.getpid()}-"
            f"{next(_RUN_COUNTER):04d}")


def _emit(progress, event):
    if progress is None:
        return
    try:
        progress(dict(event))
    except Exception:  # noqa: BLE001 - display/console progress is best effort
        pass


def _planned_months(present, planned_months_for):
    """Return a fixed, positive work plan without letting pre-scan fail a batch."""
    planned = {}
    for ticker in present:
        value = 1
        if planned_months_for is not None:
            try:
                candidate = planned_months_for(ticker)
                if (not isinstance(candidate, bool)
                        and isinstance(candidate, int) and candidate > 0):
                    value = candidate
            except Exception:  # noqa: BLE001 - failed range plans fall back to one
                pass
        planned[ticker] = value
    return planned


class _AggregateLedger:
    """Synchronous, per-batch aggregate progress ledger."""

    def __init__(self, progress, present, planned):
        self._progress = progress
        self._present = tuple(present)
        self._order = {ticker: index for index, ticker in enumerate(present)}
        self._planned = dict(planned)
        self._contribution = {ticker: 0 for ticker in present}
        self._active = set()
        self._files_done = 0
        self._last_units = 0
        self._last_emit_at = None
        # The callback stays inside this lock so concurrent workers cannot
        # deliver an older aggregate after a newer one. RLock also keeps a
        # re-entrant display callback harmless.
        self._lock = threading.RLock()

    def initial(self):
        self._publish(force=True)

    def begin(self, ticker):
        with self._lock:
            self._active.add(ticker)
            self._publish_locked()

    def observe(self, ticker, detail):
        if not isinstance(detail, dict):
            return
        month_index = detail.get("month_index")
        month_total = detail.get("month_total")
        if (isinstance(month_index, bool) or not isinstance(month_index, int)
                or isinstance(month_total, bool)
                or not isinstance(month_total, int)
                or month_index < 1 or month_total < 1):
            return
        with self._lock:
            completed = min(month_index - 1, month_total)
            candidate = self._planned[ticker] * completed // month_total
            if candidate > self._contribution[ticker]:
                self._contribution[ticker] = min(
                    candidate, self._planned[ticker])
            self._publish_locked()

    def finish(self, ticker, result, files_done, *, force=False):
        with self._lock:
            self._active.discard(ticker)
            self._files_done = max(
                self._files_done, min(int(files_done), len(self._present)))
            # Completion is determined from this ticker's normalized result,
            # never from a global cancel flag that may have changed meanwhile.
            if not result.get("cancelled"):
                self._contribution[ticker] = self._planned[ticker]
            self._publish_locked(force=force)

    def _publish(self, *, force=False):
        with self._lock:
            self._publish_locked(force=force)

    def _publish_locked(self, *, force=False):
        if self._progress is None:
            return
        now = time.monotonic()
        try:
            minimum = max(0.0, float(AGGREGATE_EMIT_MIN_S))
        except (TypeError, ValueError):
            minimum = 0.2
        if (not force and minimum > 0.0 and self._last_emit_at is not None
                and now - self._last_emit_at < minimum):
            return
        units_total = sum(self._planned.values())
        units_done = min(units_total, sum(self._contribution.values()))
        units_done = max(self._last_units, units_done)
        self._last_units = units_done
        self._last_emit_at = now
        active = sorted(self._active, key=self._order.__getitem__)[:3]
        _emit(self._progress, {
            "kind": "aggregate",
            "units_done": int(units_done),
            "units_total": int(units_total),
            "files_done": int(self._files_done),
            "files_total": len(self._present),
            "active": active,
        })


class _ItemProgress:
    """Callable raw-progress relay with a ledger-only observation channel."""

    def __init__(self, emit_raw, observe):
        self._emit_raw = emit_raw
        self._observe = observe

    def __call__(self, detail):
        observed = dict(detail) if isinstance(detail, dict) else detail
        self._emit_raw(detail)
        self._observe(observed)

    def observe(self, detail):
        observed = dict(detail) if isinstance(detail, dict) else detail
        self._observe(observed)


def progress_text(event):
    """Render one structured progress event for GUI or console use."""
    if not isinstance(event, dict):
        return str(event)
    kind = event.get("kind")
    if kind == "batch_start":
        return (f"Exporting {event.get('present', 0)} ticker file(s) to "
                f"{event.get('folder', '')}")
    if kind == "aggregate":
        units_done = int(event.get("units_done", 0) or 0)
        units_total = int(event.get("units_total", 0) or 0)
        files_done = int(event.get("files_done", 0) or 0)
        files_total = int(event.get("files_total", 0) or 0)
        percent = (100 * units_done // units_total) if units_total > 0 else 0
        text = (f"{percent}% \u2014 months {units_done}/{units_total} \u2014 "
                f"files {files_done}/{files_total}")
        active = event.get("active")
        if isinstance(active, list) and active:
            text += " \u2014 exporting " + ", ".join(str(item) for item in active[:3])
        return text
    if kind == "item_start":
        return (f"[{event.get('index', 0)}/{event.get('total', 0)}] "
                f"{event.get('ticker', '?')} -> {event.get('filename', '')}")
    if kind == "item_progress":
        detail = event.get("detail")
        if isinstance(detail, dict):
            text = (f"{event.get('ticker', '?')} {event.get('index', 0)}/"
                    f"{event.get('total', 0)} - month "
                    f"{detail.get('month_index', 0)}/"
                    f"{detail.get('month_total', 0)}")
            if detail.get("month"):
                text += f" ({detail['month']})"
            return text
        return f"{event.get('ticker', '?')}: {detail}"
    if kind == "item_done":
        return (f"[{event.get('done', 0)}/{event.get('total', 0)}] "
                f"{event.get('ticker', '?')} -> {event.get('filename', '')}")
    if kind == "note":
        return str(event.get("message") or "Quality note complete")
    return str(event.get("message") or event)


def _normalize_result(ticker, target, value, attempted):
    result = dict(value) if isinstance(value, dict) else {}
    result["ticker"] = ticker
    result["target"] = str(target)
    result["attempted"] = bool(attempted)
    if result.get("cancelled"):
        result["cancelled"] = True
        result["rows"] = 0
        return result
    if result.get("error"):
        result["error"] = _bounded(result["error"])
        result["rows"] = 0
        return result
    rows = result.get("rows", 0)
    if isinstance(rows, bool) or not isinstance(rows, int) or rows < 0:
        return {
            **result,
            "rows": 0,
            "error": "exporter returned an invalid row count",
        }
    result["rows"] = rows
    if rows > 0 and not Path(target).is_file():
        result["rows"] = 0
        result["error"] = "exporter reported rows but committed no file"
    return result


def _hole_months(holes):
    if not isinstance(holes, (list, tuple)):
        return [], bool(holes)
    months, any_hole = [], bool(holes)
    for value in holes:
        token = str(value or "")
        tail = token.rsplit(":", 1)[-1]
        if _MONTH_RE.fullmatch(tail):
            months.append(tail)
    return sorted(set(months)), any_hole


def _quality_outcomes(results, present):
    present_set = set(present)
    outcomes = {}
    for result in results:
        ticker = result["ticker"]
        if ticker not in present_set:
            continue
        if result.get("cancelled"):
            outcome = {"status": "CANCELLED", "rows": 0}
        elif result.get("error"):
            outcome = {
                "status": "FAILED",
                "rows": 0,
                "error": result["error"],
            }
        elif result.get("rows", 0) > 0:
            months, any_hole = _hole_months(result.get("holes"))
            outcome = {
                "status": "MISSING_MONTHS" if any_hole else "WRITTEN",
                "rows": result["rows"],
                "file": Path(result["target"]).name,
                "missing_months": months,
            }
            if "source_intervals" in result:
                outcome["source_intervals"] = list(result["source_intervals"])
        else:
            outcome = {"status": "EMPTY", "rows": 0}
        for key in ("start", "end"):
            if key in result:
                outcome[key] = result[key]
        outcomes[ticker] = outcome
    return outcomes


def _terminal_state(results):
    if any(result.get("cancelled") for result in results):
        return "CANCELLED"
    failures = any(result.get("error") or result.get("not_in_bank")
                   for result in results)
    completed = any(not result.get("error")
                    and not result.get("not_in_bank")
                    and not result.get("cancelled") for result in results)
    if failures:
        return "PARTIAL" if completed else "FAILED"
    return "COMPLETE"


def _summary(results):
    return {
        "files_written": sum(
            result.get("rows", 0) > 0 and not result.get("error")
            for result in results),
        "rows": sum(int(result.get("rows", 0) or 0) for result in results),
        "empty": sum(not result.get("error")
                     and not result.get("cancelled")
                     and not result.get("not_in_bank")
                     and result.get("rows", 0) == 0 for result in results),
        "failed": sum(bool(result.get("error")) for result in results),
        "cancelled": sum(bool(result.get("cancelled")) for result in results),
        "not_in_bank": sum(bool(result.get("not_in_bank")) for result in results),
        "attempted": sum(bool(result.get("attempted")) for result in results),
    }


def run_batch(storage_root, selected, present, destination, *, interval,
              file_format, sessions, start, end, filename_for, exporter,
              direct_target=None, workers=1, progress=None, cancel=None,
              now=None, batch_meta=None, quality_kwargs=None,
              planned_months_for=None):
    """Run one complete batch and return a GUI/CLI-neutral summary."""
    started = time.monotonic()
    now = _clock(now)
    root = Path(storage_root).resolve()
    if not root.is_dir():
        raise BatchExportError("storage root is not an existing directory")
    selected, present = _canonical_selection(selected, present)
    if not callable(exporter):
        raise BatchExportError("exporter must be callable")
    if planned_months_for is not None and not callable(planned_months_for):
        raise BatchExportError("planned_months_for must be callable")
    interval = str(interval or "").strip()
    if not storage.INTERVAL_RE.fullmatch(interval):
        raise BatchExportError("invalid batch interval")
    file_format = str(file_format or "").strip().lower()
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,31}", file_format):
        raise BatchExportError("invalid batch format")
    sessions = " ".join(str(sessions or "").split())
    if not sessions or len(sessions) > 64:
        raise BatchExportError("invalid batch sessions")
    start, end = str(start or ""), str(end or "")
    if len(start) > 32 or len(end) > 32:
        raise BatchExportError("invalid batch range label")
    if batch_meta is not None and not isinstance(batch_meta, dict):
        raise BatchExportError("batch metadata must be an object")
    try:
        quality_options = dict(quality_kwargs or {})
    except (TypeError, ValueError) as exc:
        raise BatchExportError("quality kwargs must be an object") from exc
    if "storage_root" in quality_options or "now" in quality_options:
        raise BatchExportError("quality kwargs cannot replace batch roots or clock")
    if (isinstance(workers, bool) or not isinstance(workers, int)
            or workers < 1 or workers > MAX_WORKERS):
        raise BatchExportError("invalid batch worker count")
    target_names = _target_names(present, filename_for)
    parent = Path(destination).resolve()
    direct_name = None
    if direct_target is not None:
        if len(selected) != 1 or len(present) != 1:
            raise BatchExportError("a direct target requires one selected ticker")
        target = Path(direct_target).resolve()
        if not target.parent.is_dir():
            raise BatchExportError("direct target parent is not a directory")
        parent = target.parent
        direct_name = _filename(target.name, present[0])
    if len(selected) == 1:
        single_name = direct_name or target_names[present[0]]
        if single_name.casefold() == batch_folder.HEALTH_REPORT_NAME.casefold():
            raise BatchExportError("output filename collides with health report")
    if _inside(parent, root):
        raise BatchExportError("export destination cannot be inside the bank")
    out_dir = data_dir = None
    try:
        out_dir, folder_created = batch_folder.prepare_destination(
            parent, len(selected), interval, file_format, now=now)
        data_dir, health_report_path = batch_folder.bundle_paths(
            out_dir, len(selected))
        if data_dir != out_dir:
            data_dir.mkdir(exist_ok=False)
        _bundle_current_health(root, health_report_path)
    except batch_folder.BatchFolderError as exc:
        if out_dir is not None:
            _remove_empty_bundle(out_dir, data_dir or out_dir)
        raise BatchExportError(str(exc)) from exc
    except BatchExportError:
        if out_dir is not None:
            _remove_empty_bundle(out_dir, data_dir or out_dir)
        raise
    except Exception as exc:  # noqa: BLE001 - normalize setup failures
        if out_dir is not None:
            _remove_empty_bundle(out_dir, data_dir or out_dir)
        raise BatchExportError(
            f"cannot prepare export bundle: {type(exc).__name__}") from exc
    targets = {
        ticker: data_dir / (direct_name if direct_name is not None
                            else target_names[ticker])
        for ticker in present
    }

    planned = _planned_months(present, planned_months_for)
    aggregate = _AggregateLedger(progress, present, planned)

    _emit(progress, {
        "kind": "health",
        "message": f"Health report: {health_report_path.name}",
        "path": str(health_report_path),
    })
    _emit(progress, {
        "kind": "batch_start",
        "selected": len(selected),
        "present": len(present),
        "folder": str(out_dir),
    })
    aggregate.initial()
    result_map = {
        ticker: {
            "ticker": ticker,
            "not_in_bank": True,
            "rows": 0,
            "attempted": False,
        }
        for ticker in selected if ticker not in set(present)
    }
    done_count = [0]

    def run_one(index, ticker):
        target = targets[ticker]
        if _cancel_requested(cancel):
            return _normalize_result(
                ticker, target, {"cancelled": True}, attempted=False)
        _emit(progress, {
            "kind": "item_start", "index": index, "total": len(present),
            "ticker": ticker, "filename": target.name,
        })
        aggregate.begin(ticker)

        def emit_item_progress(detail):
            _emit(progress, {
                "kind": "item_progress", "index": index,
                "total": len(present), "ticker": ticker,
                "filename": target.name, "detail": detail,
            })

        item_progress = _ItemProgress(
            emit_item_progress,
            lambda detail: aggregate.observe(ticker, detail))

        try:
            value = exporter(ticker, target, item_progress, cancel)
        except export_designer.ExportCancelled:
            value = {"cancelled": True}
        except Exception as exc:  # noqa: BLE001 - one ticker cannot abort batch
            value = {"error": f"{type(exc).__name__}: {exc}"}
        result = _normalize_result(ticker, target, value, attempted=True)
        return result

    try:
        if workers == 1 or len(present) == 1:
            for index, ticker in enumerate(present, 1):
                result_map[ticker] = run_one(index, ticker)
                done_count[0] += 1
                _emit(progress, {
                    "kind": "item_done", "done": done_count[0],
                    "total": len(present), "ticker": ticker,
                    "filename": targets[ticker].name,
                })
                aggregate.finish(
                    ticker, result_map[ticker], done_count[0],
                    force=done_count[0] == len(present))
        else:
            count = min(workers, len(present))
            with ThreadPoolExecutor(
                    max_workers=count, thread_name_prefix="export-batch") as pool:
                futures = {
                    pool.submit(run_one, index, ticker): ticker
                    for index, ticker in enumerate(present, 1)
                }
                for future in as_completed(futures):
                    ticker = futures[future]
                    try:
                        result_map[ticker] = future.result()
                    except Exception as exc:  # noqa: BLE001 - defensive
                        result_map[ticker] = _normalize_result(
                            ticker, targets[ticker], {
                                "error": f"{type(exc).__name__}: {exc}",
                            }, attempted=True)
                    done_count[0] += 1
                    _emit(progress, {
                        "kind": "item_done", "done": done_count[0],
                        "total": len(present), "ticker": ticker,
                        "filename": targets[ticker].name,
                    })
                    aggregate.finish(
                        ticker, result_map[ticker], done_count[0],
                        force=done_count[0] == len(present))
    except Exception:
        if folder_created:
            try:
                batch_folder.remove_created_folder_if_empty(out_dir)
            except batch_folder.BatchFolderError:
                pass
        raise

    results = [result_map[ticker] for ticker in selected]
    state = _terminal_state(results)
    summary = _summary(results)
    note_path, note_error, quality_summary = None, None, None
    report = None
    if len(selected) > 1 and summary["attempted"] > 0:
        meta = dict(batch_meta or {})
        quality_batch = {
            "run_id": meta.get("run_id") or _run_id(now),
            "format": file_format,
            "interval": interval,
            "sessions": sessions,
            "start": start,
            "end": end,
            "destination": str(out_dir),
            "state": state,
        }
        try:
            report = export_quality.build_report(
                quality_batch, selected, present,
                _quality_outcomes(results, present), storage_root=root,
                now=now, **quality_options)
            note_path = export_quality.write_report_note(
                report, out_dir, storage_root=root, now=now)
            quality_summary = report["summary"]["quality"]
            _emit(progress, {
                "kind": "note",
                "message": f"Quality note: {note_path.name}",
            })
        except Exception as exc:  # noqa: BLE001 - data files stay committed
            note_error = _bounded(f"{type(exc).__name__}: {exc}")
            _emit(progress, {
                "kind": "note",
                "message": f"Quality note failed: {note_error}",
            })

    folder_removed = False
    if folder_created and note_path is None:
        try:
            folder_removed = batch_folder.remove_created_folder_if_empty(out_dir)
        except batch_folder.BatchFolderError:
            folder_removed = False
    return {
        "kind": RESULT_KIND,
        "version": RESULT_VERSION,
        "selected": selected,
        "present": present,
        "missing": [ticker for ticker in selected if ticker not in set(present)],
        # ``folder`` historically named the directory containing the rendered
        # data files.  Keep that programmatic contract now that Row 69 adds a
        # distinct outer delivery bundle; GUI/CLI surfaces use bundle_folder.
        "folder": str(data_dir),
        "bundle_folder": str(out_dir),
        "data_folder": str(data_dir),
        "health_report": str(health_report_path),
        "folder_created": folder_created,
        "folder_removed": folder_removed,
        "format": str(file_format).lower(),
        "state": state,
        "results": results,
        "summary": summary,
        "note_path": str(note_path) if note_path is not None else None,
        "note_error": note_error,
        "quality_summary": quality_summary,
        "elapsed_seconds": round(time.monotonic() - started, 6),
    }


def _series_span(root, ticker, interval):
    from datetime import date
    try:
        canon = storage.canonical_ticker(ticker)
        manifest = storage.load_manifest(Path(root) / canon) or {}
        months = ((manifest.get("intervals") or {}).get(interval) or {}).get(
            "months") or {}
        present = sorted(
            key for key, value in months.items()
            if isinstance(value, dict) and value.get("status") == "present")
    except Exception:  # noqa: BLE001 - missing/corrupt manifest means no span
        present = []
    if not present:
        return None, None
    ey, em = (int(value) for value in present[0].split("-"))
    ly, lm = (int(value) for value in present[-1].split("-"))
    last_day = 28
    for day in (31, 30, 29, 28):
        try:
            date(ly, lm, day)
            last_day = day
            break
        except ValueError:
            continue
    return date(ey, em, 1), date(ly, lm, last_day)


def _span_union(root, ticker, intervals):
    earliest = latest = None
    for interval in intervals:
        first, last = _series_span(root, ticker, interval)
        if first is not None and (earliest is None or first < earliest):
            earliest = first
        if last is not None and (latest is None or last > latest):
            latest = last
    return earliest, latest


def _resolve_storage_range(root, ticker, interval, mode, start_date, end_date,
                           preset, hours_mode):
    """Resolve the legacy Storage Export range without reading bar data."""
    if mode != "preset":
        return start_date, end_date
    from datetime import date
    base = storage.base_interval(interval)
    combine = hours_mode == "Regular + extended (one file)"
    token = {
        "Pre-market only": f"{base}-pre",
        "After-hours only": f"{base}-post",
    }.get(hours_mode, interval)
    if combine:
        earliest, latest = _span_union(
            root, ticker, [base, f"{base}-pre", f"{base}-post"])
    else:
        earliest, latest = _series_span(root, ticker, token)
    return export_csv.preset_range(preset, date.today(), earliest, latest)


def export_storage_one(root, ticker, interval, mode, start_date, end_date,
                       preset, out_path, *, hours_mode="Regular hours",
                       progress=None, file_format="csv", cancel=None,
                       unit_progress=None, _resolved_range=_UNRESOLVED):
    """Existing Storage Export per-ticker pipeline, moved out of the GUI."""
    base = storage.base_interval(interval)
    combine = hours_mode == "Regular + extended (one file)"
    token = {
        "Pre-market only": f"{base}-pre",
        "After-hours only": f"{base}-post",
    }.get(hours_mode, interval)
    span_interval = base if combine else token
    if _resolved_range is _UNRESOLVED:
        try:
            start_date, end_date = _resolve_storage_range(
                root, ticker, interval, mode, start_date, end_date,
                preset, hours_mode)
        except storage.StorageError as exc:
            return {"error": str(exc)}
    else:
        start_date, end_date = _resolved_range
    if start_date is None or end_date is None:
        if mode == "preset":
            return {
                "error": f"no stored {span_interval} data for {ticker} to export",
            }
        return {"error": "missing start/end date"}
    if _cancel_requested(cancel):
        raise export_designer.ExportCancelled("export cancelled")
    label = f"{base} regular+pre+post" if combine else token
    if progress is not None:
        progress(f"stitching {ticker} {label} {start_date}..{end_date}")
    try:
        if file_format == "csv":
            spec = export_designer.default_spec()
            spec.update({
                "base_interval": base,
                "sessions": (["rth", "pre", "post"] if combine
                             else [storage.session_of(token)]),
                "start_date": start_date,
                "end_date": end_date,
                "file_type": "csv",
                "storage_format": True,
            })
            result = export_designer.export_one(
                root, ticker, spec, out_path, progress=unit_progress,
                cancel=cancel)
        elif combine:
            result = export_csv.export_sessions_csv(
                root, ticker, base, ["rth", "pre", "post"],
                start_date, end_date, out_path, fmt=file_format,
                progress=unit_progress)
        else:
            result = export_csv.export_combined_csv(
                root, ticker, token, start_date, end_date,
                out_path, fmt=file_format, progress=unit_progress)
    except storage.StorageError as exc:
        return {"error": str(exc)}
    result["start"] = str(start_date)
    result["end"] = str(end_date)
    result["out_path"] = str(out_path)
    if result.get("rows", 0) > 0 and "source_intervals" not in result:
        if combine:
            result["source_intervals"] = sorted(
                storage.with_session(base, session)
                for session, rows in result.get("per_session", {}).items()
                if rows > 0)
        else:
            result["source_intervals"] = [token]
    return result


def run_storage_batch(storage_root, selected, destination, *, interval,
                      mode, start_date=None, end_date=None, preset=None,
                      hours_mode="Regular hours", file_format="csv",
                      progress=None, cancel=None, now=None,
                      quality_kwargs=None):
    present, _missing = export_designer.missing_from_bank(storage_root, selected)
    base = storage.base_interval(interval)
    htag = {
        "Regular + extended (one file)": f"{base}_allhours",
        "Pre-market only": f"{base}-pre",
        "After-hours only": f"{base}-post",
    }.get(hours_mode, interval)
    range_tag = (preset if mode == "preset"
                 else f"{start_date}_{end_date}")
    extension = "parquet" if file_format == "parquet" else "csv"

    def filename_for(ticker):
        return f"{ticker}_{htag}_{range_tag}.{extension}"

    range_cache = {}

    def range_for(ticker):
        if ticker not in range_cache:
            try:
                value = _resolve_storage_range(
                    storage_root, ticker, interval, mode,
                    start_date, end_date, preset, hours_mode)
            except Exception as exc:  # noqa: BLE001 - cache exporter outcome
                range_cache[ticker] = (False, exc)
            else:
                range_cache[ticker] = (True, value)
        ok, value = range_cache[ticker]
        if not ok:
            raise value
        return value

    def planned_months_for(ticker):
        first, last = range_for(ticker)
        if first is None or last is None:
            return 1
        return len(export_designer._months_in_range(first, last))

    def exporter(ticker, target, item_progress, item_cancel):
        try:
            resolved_range = range_for(ticker)
        except storage.StorageError as exc:
            return {"error": str(exc)}
        return export_storage_one(
            storage_root, ticker, interval, mode, start_date, end_date,
            preset, target, hours_mode=hours_mode, progress=item_progress,
            file_format=file_format, cancel=item_cancel,
            unit_progress=(item_progress.observe
                           if progress is not None else None),
            _resolved_range=resolved_range)

    return run_batch(
        storage_root, selected, present, destination,
        interval=interval, file_format=extension, sessions=hours_mode,
        start=(f"preset:{preset}" if mode == "preset" else str(start_date)),
        end=("per-ticker-latest" if mode == "preset" else str(end_date)),
        filename_for=filename_for, exporter=exporter, workers=1,
        progress=progress, cancel=cancel, now=now,
        quality_kwargs=quality_kwargs,
        planned_months_for=planned_months_for)


def run_designer_batch(storage_root, selected, present, destination, spec, *,
                       direct_target=None, progress=None, cancel=None,
                       workers=None, now=None, quality_kwargs=None):
    if not isinstance(spec, dict):
        raise BatchExportError("designer spec must be an object")
    spec_copy = dict(spec)
    file_format = str(spec_copy.get("file_type") or "csv").lower()
    interval = str(spec_copy.get("base_interval") or "1m")
    sessions = "+".join(spec_copy.get("sessions") or ["rth"])
    preset = str(spec_copy.get("range_preset") or "furthest")
    if workers is None:
        workers = min(MAX_WORKERS, max(1, len(present)),
                      max(1, os.cpu_count() or 2))

    def filename_for(ticker):
        return export_designer.filename_for(ticker, spec_copy)

    range_cache = {}

    def range_for(ticker):
        if ticker not in range_cache:
            try:
                start_date = spec_copy.get("start_date")
                end_date = spec_copy.get("end_date")
                if start_date is None or end_date is None:
                    start_date, end_date = export_designer.resolve_range(
                        storage_root, ticker, interval,
                        spec_copy.get("sessions") or ["rth"], preset)
            except Exception as exc:  # noqa: BLE001 - cache exporter outcome
                range_cache[ticker] = (False, exc)
            else:
                range_cache[ticker] = (True, (start_date, end_date))
        ok, value = range_cache[ticker]
        if not ok:
            raise value
        return value

    def planned_months_for(ticker):
        start_date, end_date = range_for(ticker)
        if start_date is None or end_date is None:
            return 1
        return len(export_designer._months_in_range(start_date, end_date))

    def exporter(ticker, target, item_progress, item_cancel):
        start_date, end_date = range_for(ticker)
        if start_date is None:
            return {"rows": 0, "holes": [], "skipped": "no rows in range"}
        result = export_designer.export_one(
            storage_root, ticker,
            dict(spec_copy, start_date=start_date, end_date=end_date),
            target, progress=item_progress, cancel=item_cancel)
        result["start"] = str(start_date)
        result["end"] = str(end_date)
        return result

    fixed_start = spec_copy.get("start_date")
    fixed_end = spec_copy.get("end_date")
    return run_batch(
        storage_root, selected, present, destination,
        interval=interval, file_format=file_format, sessions=sessions,
        start=(str(fixed_start) if fixed_start is not None
               else f"preset:{preset}"),
        end=(str(fixed_end) if fixed_end is not None
             else "per-ticker-latest"),
        filename_for=filename_for, exporter=exporter,
        direct_target=direct_target, workers=workers, progress=progress,
        cancel=cancel, now=now, quality_kwargs=quality_kwargs,
        planned_months_for=planned_months_for)


__all__ = [
    "AGGREGATE_EMIT_MIN_S",
    "BatchExportError",
    "export_storage_one",
    "progress_text",
    "run_batch",
    "run_designer_batch",
    "run_storage_batch",
]
