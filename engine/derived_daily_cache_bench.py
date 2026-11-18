"""Guarded cold/warm benchmark for the Row 27b derived-daily cache.

The benchmark is offline and bank-read-only. It refuses to start while any
market-data operation owns the production gate, writes only an owned temporary
cache outside the bank, and emits one JSON artifact under Run Logs.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import tempfile
import time
from datetime import datetime
from pathlib import Path

import derived_daily_cache as cache
import operation_gate
import stock_storage as storage


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_BANK = PROJECT_ROOT / storage.STORAGE_DIR_NAME
DEFAULT_TICKERS = ("AAPL", "MSFT", "ADP", "A", "ACN", "ADBE")
ARTIFACT_VERSION = 1


class BenchmarkError(RuntimeError):
    """The benchmark precondition or structural gate failed."""


def _selected_metadata(root, tickers):
    rows = []
    for ticker in sorted(tickers):
        ticker_dir = Path(root) / storage.canonical_ticker(ticker)
        if not ticker_dir.is_dir():
            raise BenchmarkError(f"missing benchmark ticker: {ticker}")
        for path in sorted(ticker_dir.rglob("*")):
            if not path.is_file():
                continue
            stat = path.stat()
            rows.append((path.relative_to(root).as_posix(),
                         stat.st_size, stat.st_mtime_ns))
    payload = json.dumps(rows, separators=(",", ":"),
                         ensure_ascii=True).encode("ascii")
    return {
        "algorithm": "sha256",
        "sha256": hashlib.sha256(payload).hexdigest(),
        "file_count": len(rows),
    }


def _totals(rows):
    keys = (
        "current_months", "cache_hits", "cache_misses",
        "source_months_decoded", "source_rows_decoded", "days_returned",
        "invalid_cache_entries", "cache_bytes",
    )
    return {key: sum(int(row["stats"].get(key, 0)) for row in rows)
            for key in keys}


def _pass(root, tickers, cache_root):
    started = time.perf_counter()
    rows = []
    values = {}
    for ticker in tickers:
        before = time.perf_counter()
        daily, stats = cache.derive_series(
            root, ticker, "1m", cache_root=cache_root)
        rows.append({
            "ticker": storage.canonical_ticker(ticker),
            "elapsed_seconds": round(time.perf_counter() - before, 6),
            "stats": stats,
        })
        values[storage.canonical_ticker(ticker)] = daily
    return {
        "elapsed_seconds": round(time.perf_counter() - started, 6),
        "totals": _totals(rows),
        "tickers": rows,
    }, values


def _safe_remove(cache_root, parent):
    target = Path(cache_root).resolve()
    parent = Path(parent).resolve()
    if (target.parent != parent or not target.name.startswith("benchmark-")
            or not cache._is_within(target, parent)):
        raise BenchmarkError(f"refusing to remove unexpected cache path: {target}")
    shutil.rmtree(target)


def run_benchmark(root=DEFAULT_BANK, tickers=DEFAULT_TICKERS, output=None,
                  gate_path=None, cache_parent=None):
    root = Path(root).resolve()
    tickers = tuple(storage.canonical_ticker(value) for value in tickers)
    if not tickers or len(set(tickers)) != len(tickers):
        raise BenchmarkError("benchmark tickers must be unique and non-empty")
    cache_parent = Path(
        cache_parent or PROJECT_ROOT / cache.DEFAULT_CACHE_DIR_NAME).resolve()
    if cache._is_within(cache_parent, root):
        raise BenchmarkError("benchmark cache parent must stay outside the bank")
    if cache._is_within(cache_parent, PROJECT_ROOT / "Run Logs"):
        raise BenchmarkError("benchmark cache parent must stay outside Run Logs")
    lease = None
    cache_root = None
    try:
        lease = operation_gate.acquire(
            "derived_daily_benchmark", owner="Row 27b offline benchmark",
            path=gate_path)
        cache_parent.mkdir(parents=True, exist_ok=True)
        cache_root = Path(tempfile.mkdtemp(
            prefix="benchmark-", dir=cache_parent))
        started_at = datetime.now().astimezone().isoformat(timespec="seconds")
        bank_before = _selected_metadata(root, tickers)
        cold, cold_values = _pass(root, tickers, cache_root)
        warm, warm_values = _pass(root, tickers, cache_root)
        bank_after = _selected_metadata(root, tickers)
        if bank_after != bank_before:
            raise BenchmarkError("selected bank metadata changed during benchmark")
        if warm_values != cold_values:
            raise BenchmarkError("cold/warm daily output mismatch")
        cold_rows = cold["totals"]["source_rows_decoded"]
        warm_rows = warm["totals"]["source_rows_decoded"]
        if cold_rows <= 0:
            raise BenchmarkError("cold pass decoded no source rows")
        reduction = 1.0 - (warm_rows / cold_rows)
        all_current = all(
            row["stats"].get("source_current") is True
            and row["stats"].get("cacheable") is True
            for row in (*cold["tickers"], *warm["tickers"]))
        structural_pass = (
            reduction >= 0.95
            and warm_rows < cold_rows
            and warm["totals"]["cache_hits"]
            == cold["totals"]["current_months"]
            and all_current)
        if not structural_pass:
            raise BenchmarkError("warm source-row reduction gate failed")
        finished_at = datetime.now().astimezone().isoformat(timespec="seconds")
        artifact = {
            "kind": "derived_daily_cache_benchmark",
            "version": ARTIFACT_VERSION,
            "started_at": started_at,
            "finished_at": finished_at,
            "root": str(root),
            "tickers": list(tickers),
            "interval": "1m",
            "report_only": True,
            "network": False,
            "bank_written": False,
            "temporary_cache_policy": "owned benchmark directory removed on exit",
            "bank_metadata_before": bank_before,
            "bank_metadata_after": bank_after,
            "cold": cold,
            "warm": warm,
            "source_row_reduction": round(reduction, 8),
            "structural_gate": {
                "required_reduction": 0.95,
                "passed": structural_pass,
            },
        }
        destination = (Path(output).resolve() if output else
                       PROJECT_ROOT / "Run Logs" /
                       ("row27b-derived-daily-benchmark-"
                        + datetime.now().astimezone().strftime("%Y%m%d-%H%M%S")
                        + "-codex.json"))
        destination.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(
            artifact, indent=2, sort_keys=True, allow_nan=False).encode("utf-8")
        storage._atomic_write_bytes(destination, payload)
        return artifact, destination
    finally:
        try:
            if cache_root is not None:
                _safe_remove(cache_root, cache_parent)
        finally:
            if lease is not None:
                lease.release()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=str(DEFAULT_BANK))
    parser.add_argument("--tickers", default=",".join(DEFAULT_TICKERS))
    parser.add_argument("--output")
    args = parser.parse_args(argv)
    tickers = tuple(value.strip() for value in args.tickers.split(",")
                    if value.strip())
    try:
        artifact, destination = run_benchmark(
            args.root, tickers=tickers, output=args.output)
    except (BenchmarkError, operation_gate.OperationGateError,
            storage.StorageError, OSError) as exc:
        print(f"BENCHMARK BLOCKED: {type(exc).__name__}: {exc}")
        return 2
    print(json.dumps({
        "artifact": str(destination),
        "cold_seconds": artifact["cold"]["elapsed_seconds"],
        "warm_seconds": artifact["warm"]["elapsed_seconds"],
        "source_row_reduction": artifact["source_row_reduction"],
        "cache_bytes": artifact["warm"]["totals"]["cache_bytes"],
        "passed": artifact["structural_gate"]["passed"],
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
