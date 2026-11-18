"""Offline reference gates for volatility-kind gap discovery.

USAGE (Row 61 doc): a subcommand is REQUIRED — `synthetic` or `probe`.
Invoked bare, argparse exits 2: that is a USAGE error, not a failing gate, and
a batch runner must not record it as red. Batch/automated runs use `synthetic`
(status=pass; writes to a temporary directory only). `run_gates.py` encodes
exactly this contract in REFERENCE_ARGUMENT_REQUIREMENTS.

Synthetic mode writes only a temporary bank. Probe mode reads the configured bank,
never calls a gap persistence path, and refuses an output path inside that bank.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import datetime as dt
import hashlib
import json
import os
import sys
import tempfile
import time
from collections import Counter, defaultdict
from pathlib import Path

import stock_storage as ss
import stock_validate as sv
import operation_gate as og

try:                                  # harness hygiene (Row 61): never die on a glyph
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:                     # noqa: BLE001 - older/redirected streams
    pass


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_BANK = PROJECT_ROOT / "Stock Data Storage"
MAX_WORKERS = 16
INTERIOR_MISSING_FRACTION_LIMIT = 0.001
PROBE_GATE_MODE = "kind_gap_probe"


class ReferenceError(RuntimeError):
    pass


def _is_within(path, root):
    try:
        resolved_path = os.path.normcase(str(Path(path).resolve()))
        resolved_root = os.path.normcase(str(Path(root).resolve()))
        return os.path.commonpath([resolved_path, resolved_root]) == resolved_root
    except ValueError:
        return False


def _seed_series(root, ticker, interval, bars):
    by_month = defaultdict(list)
    for bar in bars:
        by_month[(bar[0].year, bar[0].month)].append(bar)
    ticker_dir = Path(root) / ticker
    manifest = ss.load_manifest(ticker_dir) or ss.new_manifest(ticker, ticker)
    months = ss.manifest_months(manifest, interval)
    for (year, month), values in sorted(by_month.items()):
        stats = ss.write_month_file(
            ss.month_file_path(root, ticker, year, month, interval), values)
        months[f"{year:04d}-{month:02d}"] = dict(stats, status="present")
    ss.save_manifest(ticker_dir, manifest)


def _daily_bars(days, *, ratio=False):
    value = 0.2 if ratio else 10.0
    volume = 0 if ratio else 100
    return [(dt.datetime.combine(day, dt.time()), value, value, value, value,
             volume) for day in days]


def _minute_bars(days, *, ratio=False, hole_day=None):
    value = 0.2 if ratio else 10.0
    volume = 0 if ratio else 100
    bars = []
    for day in days:
        for minute in range(3):
            if day == hole_day and minute == 1:
                continue
            stamp = dt.datetime.combine(day, dt.time(9, 30)) + dt.timedelta(
                minutes=minute)
            bars.append((stamp, value, value, value, value, volume))
    return bars


def _legacy_discovery(root):
    return [(ticker, interval) for ticker, interval in
            sv.discover_series(root, rth_only=False, kinds=None)
            if "-" not in interval]


def _check(checks, name, condition, detail=None):
    checks.append({
        "name": name,
        "passed": bool(condition),
        "detail": None if condition else str(detail or "condition was false"),
    })


def synthetic_gate():
    calendar_days = tuple(dt.date(2024, 1, day) for day in (2, 3, 4, 5, 8, 9))
    checks = []
    with tempfile.TemporaryDirectory(prefix="kind-gap-reference-") as temp:
        root = Path(temp) / "Stock Data Storage"
        for ticker in ("CAL1", "CAL2"):
            _seed_series(root, ticker, "1d", _daily_bars(calendar_days))

        target_price_days = tuple(day for day in calendar_days
                                  if day != dt.date(2024, 1, 4))
        target_iv_minute_days = tuple(day for day in calendar_days
                                      if day != dt.date(2024, 1, 3))
        _seed_series(root, "TARGET", "1m", _minute_bars(target_price_days))
        _seed_series(
            root, "TARGET", "1m-iv",
            _minute_bars(target_iv_minute_days, ratio=True,
                         hole_day=dt.date(2024, 1, 5)))
        _seed_series(
            root, "TARGET", "1d-iv",
            _daily_bars((dt.date(2024, 1, 4), dt.date(2024, 1, 5),
                         dt.date(2024, 1, 9), dt.date(2024, 1, 10)), ratio=True))
        _seed_series(
            root, "TARGET", "1d-hvol",
            _daily_bars((dt.date(2024, 1, 3), dt.date(2024, 1, 4),
                         dt.date(2024, 1, 8), dt.date(2024, 1, 9),
                         dt.date(2024, 1, 10)), ratio=True))

        calendar = sv.consensus_calendar(
            root, read_fn=sv.read_series, min_tickers=2)
        _check(checks, "calendar uses TRADES daily series only",
               calendar == set(calendar_days), sorted(calendar))
        _check(checks, "volatility-only day cannot enter consensus calendar",
               dt.date(2024, 1, 10) not in calendar)

        default_scan = sv.scan_all_gaps(
            root, write=False, calendar_days=calendar)
        legacy_scan = sv.scan_all_gaps(
            root, series=_legacy_discovery(root), write=False,
            calendar_days=calendar)
        _check(checks, "default scan is byte-identical to legacy discovery",
               default_scan == legacy_scan)

        iv_scan = sv.scan_all_gaps(
            root, write=False, calendar_days=calendar, kinds=("iv",))
        hvol_scan = sv.scan_all_gaps(
            root, write=False, calendar_days=calendar, kinds=("hvol",))
        all_scan = sv.scan_all_gaps(
            root, write=False, calendar_days=calendar,
            kinds=("", "iv", "hvol"))

        _check(checks, "IV selection includes exactly IV series",
               set(iv_scan["summary"]) == {
                   "TARGET 1d-iv", "TARGET 1m-iv"}, iv_scan["summary"])
        _check(checks, "HVOL selection includes exactly HVOL series",
               set(hvol_scan["summary"]) == {"TARGET 1d-hvol"},
               hvol_scan["summary"])
        _check(checks, "combined selection includes every requested kind",
               set(all_scan["summary"]) == set(default_scan["summary"]) | {
                   "TARGET 1d-iv", "TARGET 1m-iv", "TARGET 1d-hvol"})

        iv_minute = iv_scan["summary"]["TARGET 1m-iv"]
        iv_daily = iv_scan["summary"]["TARGET 1d-iv"]
        hvol_daily = hvol_scan["summary"]["TARGET 1d-hvol"]
        _check(checks, "1m-IV planted whole-day gap is exact",
               iv_minute.get("missing_day_list") == ["2024-01-03"], iv_minute)
        _check(checks, "1m-IV planted interior gap is exact",
               iv_minute.get("missing_total") == 1, iv_minute)
        _check(checks, "1d-IV clamp excludes pre-coverage days",
               iv_daily.get("missing_day_list") == ["2024-01-08"], iv_daily)
        _check(checks, "1d-HVOL clamp excludes pre-coverage days",
               hvol_daily.get("missing_day_list") == ["2024-01-05"], hvol_daily)

        ratio_bars = (
            sv.read_series(root, "TARGET", "1m-iv")
            + sv.read_series(root, "TARGET", "1d-iv")
            + sv.read_series(root, "TARGET", "1d-hvol"))
        ratio_shape = all(
            0 < value < 1 and bar[5] == 0
            for bar in ratio_bars for value in bar[1:5])
        _check(checks, "ratio OHLC and stored sentinel volume round-trip",
               ratio_shape and bool(ratio_bars))

        gate_calls = []
        real_acquire = og.acquire

        class _SyntheticLease:
            mode = PROBE_GATE_MODE
            path = str(Path(temp) / "synthetic-operation-gate.lock")

            def __enter__(self):
                return self

            def __exit__(self, _exc_type, _exc, _tb):
                return False

        def fake_acquire(mode, *, owner=None, path=None):
            gate_calls.append((mode, owner, path))
            return _SyntheticLease()

        stale_day = dt.date(1999, 1, 4).toordinal() - sv._EPOCH_ORD
        stale_cache = {
            "tickers": {
                f"{ticker} 1d": {"key": "stale", "days": [stale_day]}
                for ticker in ("CAL1", "CAL2")
            }
        }
        stale_cache["tickers"]["GHOST 1d"] = {
            "key": "ghost", "days": [stale_day]}
        ss._atomic_write_bytes(
            root / sv._CAL_FILE,
            json.dumps(stale_cache, sort_keys=True).encode("utf-8"))
        probe_artifact = Path(temp) / "synthetic-probe.json"
        try:
            og.acquire = fake_acquire
            probe = guarded_real_bank_probe(
                root, output=probe_artifact, workers=2)
        finally:
            og.acquire = real_acquire
        _check(checks, "real-bank probe holds the whole-run operation gate",
               gate_calls == [(PROBE_GATE_MODE,
                               "row30-kind-gap-read-only-probe", None)],
               gate_calls)
        _check(checks, "guarded probe writes only its outside-bank artifact",
               probe.get("status") == "pass"
               and probe.get("read_only_guard_unchanged") is True
               and probe_artifact.is_file()
               and not _is_within(probe_artifact, root), probe)
        calendar_evidence = probe.get("calendar_evidence") or {}
        _check(checks, "stale/ghost calendar cache entries are not probe evidence",
               probe.get("calendar_days") == len(calendar_days)
               and calendar_evidence.get("cache_hits") == 0
               and calendar_evidence.get("cache_misses") == 2,
               calendar_evidence)

        ghost_root = Path(temp) / "Ghost Bank"
        ghost_root.mkdir()
        ss._atomic_write_bytes(
            ghost_root / sv._CAL_FILE,
            json.dumps(stale_cache, sort_keys=True).encode("utf-8"))
        try:
            _read_only_calendar(ghost_root)
            ghost_rejected = False
        except ReferenceError:
            ghost_rejected = True
        _check(checks, "cache-only ghost tickers cannot create a bank calendar",
               ghost_rejected)

    return {
        "kind": "kind_gap_reference_synthetic",
        "status": "pass" if all(item["passed"] for item in checks) else "fail",
        "checks": checks,
        "check_count": len(checks),
        "writes": "temporary directory only",
    }


def _hash_file(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        while True:
            chunk = handle.read(1 << 20)
            if not chunk:
                return digest.hexdigest()
            digest.update(chunk)


def _file_stamp(path):
    path = Path(path)
    if not path.exists():
        return {"exists": False}
    stat = path.stat()
    return {
        "exists": True,
        "size": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
        "sha256": _hash_file(path),
    }


def _guard_snapshot(root):
    root = Path(root)
    manifests = sorted(root.glob(f"*/{ss.MANIFEST_NAME}"))
    digest = hashlib.sha256()
    for path in manifests:
        stat = path.stat()
        digest.update(path.relative_to(root).as_posix().encode("utf-8"))
        digest.update(f":{stat.st_size}:{stat.st_mtime_ns}:".encode("ascii"))
        digest.update(_hash_file(path).encode("ascii"))
        digest.update(b"\n")
    guarded = [
        root / sv._CAL_FILE,
        root / sv._GAPS_FILE,
        root.parent / sv.GAP_REPORT_NAME,
    ]
    return {
        "manifest_count": len(manifests),
        "manifest_tree_sha256": digest.hexdigest(),
        "sidecars": {str(path.resolve()): _file_stamp(path) for path in guarded},
    }


def _read_only_calendar(root, min_tickers=2):
    root = Path(root)
    cache_path = root / sv._CAL_FILE
    cache = {}
    cache_sha256 = None
    try:
        raw = json.loads(cache_path.read_text(encoding="utf-8"))
        entries = raw.get("tickers", {}) if isinstance(raw, dict) else {}
        if isinstance(entries, dict):
            cache = entries
            cache_sha256 = _hash_file(cache_path)
    except (OSError, ValueError, TypeError):
        cache = {}

    counts = Counter()
    accepted = 0
    cache_hits = 0
    cache_misses = 0
    for ticker, interval in sv.discover_series(
            root, rth_only=True, kinds=("",)):
        if ss.base_interval(interval) != "1d":
            continue
        canon = ss.canonical_ticker(ticker)
        manifest = ss.load_manifest(root / canon) or {}
        fingerprint = sv._cal_fingerprint(
            ss.manifest_months(manifest, interval))
        entry = cache.get(f"{canon} {interval}")
        days = entry.get("days") if isinstance(entry, dict) else None
        if (isinstance(entry, dict)
                and entry.get("key") == fingerprint
                and isinstance(days, list)
                and all(isinstance(day, int) and not isinstance(day, bool)
                        for day in days)):
            cache_hits += 1
        else:
            try:
                days = sorted({value // 86400 for value in
                               sv.read_series_ts(root, ticker, interval)})
            except Exception as exc:  # noqa: BLE001 - evidence must be complete
                raise ReferenceError(
                    f"could not read current calendar series {canon} "
                    f"{interval}: {exc}") from exc
            cache_misses += 1
        accepted += 1
        for day in days:
            counts[day] += 1
    calendar = {
        dt.date.fromordinal(sv._EPOCH_ORD + day)
        for day, count in counts.items() if count >= min_tickers
    }
    if not calendar:
        raise ReferenceError("could not build a read-only TRADES consensus calendar")
    return calendar, {
        "source": ("validated_existing_consensus_cache"
                   if cache_misses == 0
                   else "validated_cache_with_read_only_rebuild"),
        "ticker_entries": accepted,
        "cache_hits": cache_hits,
        "cache_misses": cache_misses,
        "cache_sha256": cache_sha256,
    }


def _scan_one(root, calendar, pair):
    ticker, interval = pair
    return pair, sv.scan_series_gaps(
        root, ticker, interval, calendar_days=calendar, max_report=0)


def _empty_metrics():
    return {
        "series_discovered": 0,
        "series_scanned": 0,
        "series_failed": 0,
        "bars": 0,
        "interior_gap_events": 0,
        "interior_missing_bars": 0,
        "series_with_interior_gaps": 0,
        "whole_missing_days": 0,
        "source_absent_days": 0,
    }


def _aggregate_probe(series, results):
    groups = defaultdict(_empty_metrics)
    for key in ("iv", "hvol", "1m-iv", "1d-iv", "1d-hvol"):
        groups[key]
    failures = []
    for ticker, interval in series:
        kind = ss.kind_of(interval) or "trades"
        for key in (kind, interval):
            groups[key]["series_discovered"] += 1
    for (ticker, interval), scan in results:
        kind = ss.kind_of(interval) or "trades"
        targets = (groups[kind], groups[interval])
        if scan.get("error"):
            for metrics in targets:
                metrics["series_failed"] += 1
            if len(failures) < 50:
                failures.append({
                    "ticker": ticker, "interval": interval,
                    "error": str(scan["error"])[:500],
                })
            continue
        for metrics in targets:
            metrics["series_scanned"] += 1
            metrics["bars"] += int(scan.get("bars", 0))
            metrics["interior_gap_events"] += int(scan.get("gap_count", 0))
            metrics["interior_missing_bars"] += int(
                scan.get("missing_total", 0))
            metrics["series_with_interior_gaps"] += bool(
                scan.get("gap_count", 0))
            metrics["whole_missing_days"] += int(
                scan.get("missing_day_count", 0))
            metrics["source_absent_days"] += int(
                scan.get("source_absent_count", 0))
    for metrics in groups.values():
        denominator = metrics["bars"] + metrics["interior_missing_bars"]
        metrics["interior_missing_fraction"] = (
            metrics["interior_missing_bars"] / denominator
            if denominator else None)
    return dict(sorted(groups.items())), failures


def _interior_recommendation(metrics):
    if not metrics or not metrics["series_discovered"]:
        return {
            "policy": "unavailable",
            "reason": "the bank contains no 1m-iv series",
        }
    coverage = metrics["series_scanned"] / metrics["series_discovered"]
    fraction = metrics.get("interior_missing_fraction")
    if (coverage >= 0.99 and fraction is not None
            and fraction <= INTERIOR_MISSING_FRACTION_LIMIT):
        return {
            "policy": "interior_and_missing_days",
            "reason": ("observed interior missing fraction is at or below "
                       f"{INTERIOR_MISSING_FRACTION_LIMIT:.4%} with at least "
                       "99% of series scanned"),
        }
    return {
        "policy": "missing_days_only",
        "reason": ("interior density or scan coverage exceeds the conservative "
                   "reference threshold"),
    }


def real_bank_probe(root, *, workers=4):
    root = Path(root).resolve()
    if not root.is_dir():
        raise ReferenceError(f"bank is not a directory: {root}")
    if not 1 <= workers <= MAX_WORKERS:
        raise ReferenceError(f"workers must be between 1 and {MAX_WORKERS}")

    before = _guard_snapshot(root)
    started = time.monotonic()
    calendar, calendar_evidence = _read_only_calendar(root)
    series = sv.discover_series(
        root, rth_only=True, kinds=("iv", "hvol"))
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(_scan_one, root, calendar, pair)
                   for pair in series]
        for future in concurrent.futures.as_completed(futures):
            results.append(future.result())
    results.sort(key=lambda item: item[0])
    metrics, failures = _aggregate_probe(series, results)
    after = _guard_snapshot(root)
    unchanged = before == after
    if not unchanged:
        raise ReferenceError("read-only guard changed while the probe was running")

    recommendation = _interior_recommendation(metrics.get("1m-iv"))
    return {
        "kind": "kind_gap_reference_real_bank_probe",
        "status": "pass" if not failures else "fail",
        "bank": str(root),
        "write_mode": False,
        "workers": workers,
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "calendar_days": len(calendar),
        "calendar_evidence": calendar_evidence,
        "series_total": len(series),
        "metrics": metrics,
        "failures": failures,
        "failure_count": sum(
            value["series_failed"] for key, value in metrics.items()
            if key in {"iv", "hvol"}),
        "one_minute_iv_interior_policy": recommendation,
        "interior_fraction_limit": INTERIOR_MISSING_FRACTION_LIMIT,
        "read_only_guard_unchanged": unchanged,
        "guard": after,
    }


def guarded_real_bank_probe(root, *, output, workers=4):
    """Run one read-only bank probe under the cross-process operation gate.

    The lease prevents every cooperating fetch/Fix-data/external sweep from
    starting between the before/after bank fingerprints.  The fingerprints
    remain the fail-closed backstop for any writer that does not use the gate.
    The sole output is an atomically written JSON artifact outside the bank.
    """
    bank = Path(root).resolve()
    artifact = Path(output).resolve()
    if _is_within(artifact, bank):
        raise ReferenceError("probe output must stay outside the bank")
    with og.acquire(
            PROBE_GATE_MODE,
            owner="row30-kind-gap-read-only-probe") as lease:
        payload = real_bank_probe(bank, workers=workers)
        payload["artifact"] = str(artifact)
        payload["operation_gate"] = {
            "mode": lease.mode,
            "path": lease.path,
        }
        _write_json(artifact, payload)
    return payload


def _write_json(path, payload):
    path = Path(path).resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("x", encoding="utf-8", newline="\n") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True, ensure_ascii=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
    return path


def _parser():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("synthetic")
    probe = commands.add_parser("probe")
    probe.add_argument("--bank", default=str(DEFAULT_BANK))
    probe.add_argument("--workers", type=int, default=4)
    probe.add_argument("--output", required=True)
    return parser


def main(argv=None):
    try:
        args = _parser().parse_args(argv)
        if args.command == "synthetic":
            payload = synthetic_gate()
            exit_code = 0 if payload["status"] == "pass" else 1
        else:
            payload = guarded_real_bank_probe(
                args.bank, output=args.output, workers=args.workers)
            exit_code = 0 if payload["status"] == "pass" else 1
    except Exception as exc:  # noqa: BLE001 - bounded CLI error envelope
        payload = {
            "kind": "kind_gap_reference_error",
            "status": "error",
            "error": {
                "type": type(exc).__name__,
                "message": (str(exc) or "unknown error")[:1000],
            },
        }
        exit_code = 2
    print(json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=True))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
