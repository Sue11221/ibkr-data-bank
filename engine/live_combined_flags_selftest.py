"""Offline persistence tests for the three-source live producer.

Only a temporary fixture bank is written. The fake adapter makes no IBKR call,
and the StockAnalysis fetcher is replaced with deterministic fixture data.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import live_combined_flags as live  # noqa: E402
import stock_storage as storage  # noqa: E402


FAILS = []
TOTAL = [0]


def check(name, condition, detail=""):
    TOTAL[0] += 1
    ok = bool(condition)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}"
          + (f" -- {detail}" if detail and not ok else ""))
    if not ok:
        FAILS.append(name)


def _bars(day, count=60):
    start = dt.datetime.combine(day, dt.time(9, 30))
    return [
        (start + dt.timedelta(minutes=index),
         100.0 + index * 0.01, 100.1 + index * 0.01,
         99.9 + index * 0.01, 100.05 + index * 0.01, 1000 + index)
        for index in range(count)
    ]


def _seed_1m(root):
    ticker_dir = Path(root) / "AAA"
    ticker_dir.mkdir(parents=True)
    manifest = storage.new_manifest("AAA", "AAA")
    bars = _bars(dt.date(2026, 6, 1))
    path = storage.month_file_path(root, "AAA", 2026, 6, "1m")
    storage.manifest_months(manifest, "1m")["2026-06"] = dict(
        storage.write_month_file(path, bars), status="present")
    storage.save_manifest(ticker_dir, manifest)
    return bars


def _seed_1d(root, daily):
    ticker_dir = Path(root) / "AAA"
    manifest = storage.load_manifest(ticker_dir)
    path = storage.month_file_path(root, "AAA", 2026, 6, "1d")
    storage.manifest_months(manifest, "1d")["2026-06"] = dict(
        storage.write_month_file(path, daily), status="present")
    storage.save_manifest(ticker_dir, manifest)


def _data_snapshot(root):
    paths = sorted(
        path for path in Path(root).rglob("*")
        if path.is_file() and path.name != "_combined_flags.json")
    return {
        path.relative_to(root).as_posix(): hashlib.sha256(
            path.read_bytes()).hexdigest()
        for path in paths
    }


def _reference_fixture(root, external, *, failure=None, require_persisted=False):
    """Requestless persistence component; Operations owns actual IBKR-path tests."""
    def compare():
        if failure is not None:
            raise failure
        daily = live.sv.read_series(root, "AAA", "1d")
        internal = live.sv.parse_ibkr_daily_reference(daily) if daily else {}
        verdict = live.sv.combined_crosscheck(
            root, "AAA", "1m", ext_ref=external, int_ref=internal,
            requested_range="15Y")
        if not internal or not external:
            verdict["status"] = "inconclusive"
            verdict["note"] = ("missing " + ("stored-1d " if not internal else "")
                               + ("stockanalysis" if not external else "")).strip()
        return verdict
    result = live.sv.combined_crosscheck_with_provenance(root, "AAA", "1m", compare)
    written = live.sv.record_combined_flags(root, result)
    if require_persisted and written is None:
        raise RuntimeError("combined cross-check result was not saved")
    return result


def run():
    original_record = live.sv.record_combined_flags
    try:
        with tempfile.TemporaryDirectory(prefix="combined-producer-") as tmp:
            root = Path(tmp) / "bank"
            bars = _seed_1m(root)
            derived = live.sv.derive_daily(bars)
            external = {
                day.isoformat(): values for day, values in derived.items()
            }
            before = _data_snapshot(root)

            missing_daily = _reference_fixture(root, external)
            missing_daily_internal = missing_daily.get(
                "internal_interval_fingerprint", {})
            check("missing stored daily is persisted as inconclusive",
                  missing_daily["status"] == "inconclusive"
                  and missing_daily["interval_fingerprint"]["current"] is True
                  and missing_daily_internal.get("current") is False
                  and missing_daily_internal.get("observed_status")
                  == "inconclusive"
                  and "stored-1d" in missing_daily_internal.get(
                      "observed_note", ""),
                  str(missing_daily))
            check("missing-daily run leaves stored data unchanged",
                  before == _data_snapshot(root))

            day, values = next(iter(derived.items()))
            daily = [(dt.datetime.combine(day, dt.time()), *values)]
            _seed_1d(root, daily)
            before_external_missing = _data_snapshot(root)
            missing_external = _reference_fixture(root, {})
            check("missing external reference is persisted as inconclusive",
                  missing_external["status"] == "inconclusive"
                  and "stockanalysis" in missing_external.get("note", ""),
                  str(missing_external))

            failed = _reference_fixture(
                root, {}, failure=RuntimeError("qualification unavailable"))
            stored = (live.sv.combined_entries_for_ticker(
                live.sv.load_combined_flags(root), "AAA") or [{}])[0]
            check("qualification failure supersedes prior evidence visibly",
                  failed["status"] == "error"
                  and stored.get("status") == "error"
                  and "qualification unavailable" in stored.get("note", "")
                  and stored.get("interval_fingerprint", {}).get("current") is True,
                  str(stored))
            check("producer writes no manifest or market-data bytes",
                  before_external_missing == _data_snapshot(root),
                  str((before, before_external_missing, _data_snapshot(root))))

            live.sv.record_combined_flags = lambda *_args, **_kwargs: None
            try:
                _reference_fixture(root, {}, require_persisted=True)
            except RuntimeError as exc:
                persistence_failed = "not saved" in str(exc)
            else:
                persistence_failed = False
            check("required persistence fails closed for Fix Data debt credit",
                  persistence_failed)
    finally:
        live.sv.record_combined_flags = original_record

    print(f"\nlive_combined_flags_selftest: "
          f"{TOTAL[0] - len(FAILS)}/{TOTAL[0]} passed, {len(FAILS)} failed")
    return 1 if FAILS else 0


if __name__ == "__main__":
    sys.exit(run())
