"""Offline crash/recovery checks for ordinary correction-bearing rewrites.

Both real writer seams are exercised against TemporaryDirectory banks.  Child
processes terminate with ``os._exit`` at durable transaction phases; no
adapter, socket, listener, GUI, network, or production path is used.

Run: python -B engine/ordinary_correction_write_selftest.py
"""

from __future__ import annotations

import copy
import hashlib
import json
import os
import subprocess
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace


sys.path.insert(0, str(Path(__file__).resolve().parent))

import stock_ibkr as ibkr  # noqa: E402
import stock_ingest as ingest  # noqa: E402
import stock_storage as storage  # noqa: E402


CHECKS = 0
FAILURES: list[str] = []
INTERVAL = "1m-iv"
KEY = "2024-06"
DECOY_KEY = "2024-05"
LEDGER = [{
    "day": "2024-06-03", "stored": 0.2, "served": 0.2,
    "request_id": "ordinary-wal-selftest",
}]
DECOY_LEDGER = [{
    "day": "2024-05-03", "stored": 0.19, "served": 0.19,
    "request_id": "ordinary-wal-decoy",
}]
PHASES = ("sentinel_temp", "backup_temp", "prepared", "marker", "manifest",
          "month", "csv", "cleanup_marker", "cleanup_sentinel")


def check(name, condition, detail=""):
    global CHECKS
    CHECKS += 1
    ok = bool(condition)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}"
          + (f" -- {detail}" if detail and not ok else ""))
    if not ok:
        FAILURES.append(name)


def bar(minute, value, *, month=6):
    stamp = datetime(2024, month, 3, 9, 30 + minute)
    value = float(value)
    return (stamp, value, value, value, value, 0)


def seed(root, ticker, *, fmt="csv"):
    old_path = storage.month_file_path(
        root, ticker, 2024, 6, INTERVAL, fmt=fmt)
    stats = storage.write_month_file(old_path, [bar(0, 0.2)])
    manifest = storage.new_manifest(ticker, ticker)
    entry = dict(stats, status="present", source="selftest")
    entry["value_corrections"] = copy.deepcopy(LEDGER)
    storage.manifest_months(manifest, INTERVAL)[KEY] = entry
    decoy_path = storage.month_file_path(
        root, ticker, 2024, 5, INTERVAL)
    decoy_stats = storage.write_month_file(
        decoy_path, [bar(0, 0.19, month=5)])
    decoy_entry = dict(decoy_stats, status="present", source="decoy")
    decoy_entry["value_corrections"] = copy.deepcopy(DECOY_LEDGER)
    storage.manifest_months(manifest, INTERVAL)[DECOY_KEY] = decoy_entry
    storage.save_manifest(root / ticker, manifest)
    return old_path, stats


def result_shell():
    return {
        "blocked_months": [], "dup_existing": 0, "conflicts": 0,
        "months": {}, "added": 0, "written": 0, "notes": [],
    }


def run_ibkr(root, ticker):
    result = result_shell()
    state = {
        "manifest": storage.load_manifest(root / ticker),
        "since_save": 0,
    }
    ibkr._commit_month(
        root, ticker, INTERVAL, (2024, 6), [bar(1, 0.21)],
        "ordinary-wal-selftest", "IBKR:selftest", result,
        lambda *_args: None, mstate=state)
    return result


def run_ingest(root, ticker):
    source = SimpleNamespace(
        path=Path(f"{ticker}_selftest.csv"),
        raw_symbols={ticker: {ticker}},
        prewrite_month_shas={}, merge={}, notes=[])
    with storage.ticker_transaction(root / ticker):
        result = ingest._merge_series(
            root, source, ticker, INTERVAL, [bar(1, 0.21)],
            "ordinary-wal-selftest", lambda *_args: None, None)
        source.merge[ticker] = result
        if result.get("written") and not result.get("recovery_pending"):
            ingest._update_manifest(
                root, source, ticker, "ordinary-wal-selftest")
    return result


def run_writer(writer, root, ticker):
    return run_ibkr(root, ticker) if writer == "ibkr" \
        else run_ingest(root, ticker)


def coherent(root, ticker, expected):
    tdir = root / ticker
    manifest = storage.load_manifest(tdir)
    if manifest is None:
        return False, "manifest missing"
    entry = (((manifest.get("intervals") or {}).get(INTERVAL) or {})
             .get("months") or {}).get(KEY) or {}
    decoy_entry = (((manifest.get("intervals") or {}).get(INTERVAL) or {})
                   .get("months") or {}).get(DECOY_KEY) or {}
    decoy_path = storage.find_month_file(root, ticker, 2024, 5, INTERVAL)
    active = storage.find_month_file(root, ticker, 2024, 6, INTERVAL)
    if active is None:
        return False, "active month missing"
    sha = hashlib.sha256(Path(active).read_bytes()).hexdigest()
    bars, _stats = storage.read_month_file(active)
    stage = tdir / storage.VOL_VALUE_RECONCILE_STAGE_DIR
    ok = (entry.get("sha256") == sha
          and entry.get("value_corrections") == LEDGER
          and decoy_path is not None
          and decoy_entry.get("sha256") == hashlib.sha256(
              Path(decoy_path).read_bytes()).hexdigest()
          and decoy_entry.get("value_corrections") == DECOY_LEDGER
          and len(bars) == (1 if expected == "old" else 2)
          and Path(active).suffix == (".csv" if expected == "old"
                                      else ".parquet")
          and not stage.exists())
    return ok, (
        f"entry={entry}, active={active}, rows={len(bars)}, "
        f"stage={stage.exists()}")


def child_main(writer, root_text, ticker, phase):
    root = Path(root_text)

    if phase in {"sentinel_temp", "backup_temp"}:
        real_replace = os.replace

        def exit_during_atomic(source, target):
            target = Path(target)
            if (phase == "sentinel_temp"
                    and target.name
                    == storage.ORDINARY_CORRECTION_TXN_SENTINEL):
                os._exit(73)
            if (phase == "backup_temp"
                    and target.name == storage._ORDINARY_TXN_MONTH_BEFORE):
                os._exit(73)
            return real_replace(source, target)

        os.replace = exit_during_atomic
    elif phase == "cleanup_sentinel":
        real_unlink = Path.unlink

        def exit_after_sentinel_unlink(path, *args, **kwargs):
            if (path.name == storage.ORDINARY_CORRECTION_TXN_SENTINEL
                    and path.parent.name
                    == storage.VOL_VALUE_RECONCILE_STAGE_DIR):
                result = real_unlink(path, *args, **kwargs)
                os._exit(73)
                return result
            return real_unlink(path, *args, **kwargs)

        Path.unlink = exit_after_sentinel_unlink

    def hard_exit(seen):
        if seen == phase:
            os._exit(73)

    storage._ordinary_correction_txn_hook = hard_exit
    run_writer(writer, root, ticker)
    os._exit(74)


def hard_exit_matrix(base):
    script = str(Path(__file__).resolve())
    for writer in ("ibkr", "ingest"):
        for phase in PHASES:
            root = Path(base) / f"crash-{writer}-{phase}" \
                / storage.STORAGE_DIR_NAME
            root.mkdir(parents=True)
            ticker = "IBW" if writer == "ibkr" else "IGW"
            seed(root, ticker)
            proc = subprocess.run(
                [sys.executable, "-B", script, "--child", writer,
                 str(root), ticker, phase],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                timeout=30)
            check(f"{writer} {phase}: child reached os._exit",
                  proc.returncode == 73,
                  f"rc={proc.returncode}, out={proc.stdout}, err={proc.stderr}")
            scan1 = storage.scan_storage(root, workers=1)
            expected = "old" if phase in {
                "sentinel_temp", "backup_temp", "prepared", "marker",
                "manifest"} else "new"
            ok, detail = coherent(root, ticker, expected)
            check(f"{writer} {phase}: recovery is coherent with ledger", ok,
                  f"{detail}; scan={scan1}")
            scan2 = storage.scan_storage(root, workers=1)
            ok, detail = coherent(root, ticker, expected)
            check(f"{writer} {phase}: second scan cannot erase ledger",
                  ok and not scan2["errors"], f"{detail}; scan={scan2}")


def manifest_failure(base, writer):
    root = Path(base) / f"manifest-failure-{writer}" \
        / storage.STORAGE_DIR_NAME
    root.mkdir(parents=True)
    ticker = "IBF" if writer == "ibkr" else "IGF"
    seed(root, ticker)
    tdir = root / ticker
    real_atomic = storage._atomic_write_bytes
    failed = {"done": False}

    def fail_manifest(target, payload):
        target = Path(target)
        if target == tdir / storage.MANIFEST_NAME and not failed["done"]:
            failed["done"] = True
            raise storage.StorageError("injected authoritative manifest fault")
        return real_atomic(target, payload)

    storage._atomic_write_bytes = fail_manifest
    try:
        result = run_writer(writer, root, ticker)
    finally:
        storage._atomic_write_bytes = real_atomic
    stage = tdir / storage.VOL_VALUE_RECONCILE_STAGE_DIR
    check(f"{writer}: injected manifest fault leaves recovery debt",
          failed["done"] and stage.exists(), str(result))
    storage.scan_storage(root, workers=1)
    ok, detail = coherent(root, ticker, "old")
    check(f"{writer}: scanner recovers manifest fault without ledger loss",
          ok, detail)


def csv_unlink_failure(base, writer):
    root = Path(base) / f"csv-failure-{writer}" / storage.STORAGE_DIR_NAME
    root.mkdir(parents=True)
    ticker = "IBC" if writer == "ibkr" else "IGC"
    csv_path, _stats = seed(root, ticker)
    tdir = root / ticker
    real_unlink = Path.unlink
    failed = {"done": False}

    def fail_csv(self, *args, **kwargs):
        if Path(self) == csv_path and not failed["done"]:
            failed["done"] = True
            raise PermissionError("injected CSV twin lock")
        return real_unlink(self, *args, **kwargs)

    Path.unlink = fail_csv
    try:
        result = run_writer(writer, root, ticker)
    finally:
        Path.unlink = real_unlink
    stage = tdir / storage.VOL_VALUE_RECONCILE_STAGE_DIR
    check(f"{writer}: CSV unlink fault remains durable recovery debt",
          failed["done"] and stage.exists() and csv_path.exists(), str(result))
    storage.scan_storage(root, workers=1)
    ok, detail = coherent(root, ticker, "new")
    check(f"{writer}: CSV unlink retry commits and preserves ledger", ok,
          detail)


def leave_csv_cleanup_debt(base, name, ticker):
    root = Path(base) / name / storage.STORAGE_DIR_NAME
    root.mkdir(parents=True)
    csv_path, _stats = seed(root, ticker)
    real_unlink = Path.unlink
    failed = {"done": False}

    def fail_csv(self, *args, **kwargs):
        if Path(self) == csv_path and not failed["done"]:
            failed["done"] = True
            raise PermissionError("injected CSV twin lock")
        return real_unlink(self, *args, **kwargs)

    Path.unlink = fail_csv
    try:
        result = run_ibkr(root, ticker)
    finally:
        Path.unlink = real_unlink
    stage = root / ticker / storage.VOL_VALUE_RECONCILE_STAGE_DIR
    check(f"{name}: fixture leaves committed CSV cleanup debt",
          failed["done"] and csv_path.exists() and stage.exists(),
          str(result))
    return root, csv_path, stage


def leave_parquet_cleanup_debt(base, name, ticker):
    root = Path(base) / name / storage.STORAGE_DIR_NAME
    root.mkdir(parents=True)
    parquet_path, _stats = seed(root, ticker, fmt="parquet")
    real_clear = storage._clear_ordinary_correction_stage
    failed = {"done": False}

    def fail_stage_clear(*_args, **_kwargs):
        failed["done"] = True
        raise storage.StorageError("injected committed-stage cleanup failure")

    storage._clear_ordinary_correction_stage = fail_stage_clear
    try:
        result = run_ibkr(root, ticker)
    finally:
        storage._clear_ordinary_correction_stage = real_clear
    stage = root / ticker / storage.VOL_VALUE_RECONCILE_STAGE_DIR
    check(f"{name}: fixture leaves committed Parquet cleanup debt",
          failed["done"] and parquet_path.exists() and stage.exists(),
          str(result))
    return root, parquet_path, stage


def marker_path_tampering(base):
    ticker = "PATHWAL"
    root, csv_path, stage = leave_csv_cleanup_debt(
        base, "marker-path-tamper", ticker)
    tdir = root / ticker
    marker_path = stage / storage.ORDINARY_CORRECTION_TXN_MARKER
    marker_raw = marker_path.read_bytes()
    marker = json.loads(marker_raw.decode("utf-8"))
    victim = tdir / "victim.csv"
    victim_raw = b"unrelated user evidence\r\n"
    victim.write_bytes(victim_raw)
    marker["old_rel"] = victim.name
    storage._atomic_write_bytes(
        marker_path, json.dumps(
            marker, sort_keys=True, separators=(",", ":")).encode("utf-8"))

    scan = storage.scan_storage(root, workers=1)
    check("tampered old_rel cannot delete an unrelated in-ticker CSV",
          victim.read_bytes() == victim_raw and csv_path.exists()
          and stage.exists(), str(scan))
    check("tampered old_rel remains visible recovery debt",
          bool(scan["errors"]), str(scan))

    storage._atomic_write_bytes(marker_path, marker_raw)
    storage.scan_storage(root, workers=1)
    ok, detail = coherent(root, ticker, "new")
    check("restored exact marker recovers and preserves ledger", ok, detail)
    check("successful recovery never touched the unrelated CSV",
          victim.read_bytes() == victim_raw)


def marker_format_tampering(base):
    csv_ticker = "CSVFMTWAL"
    csv_root, csv_path, csv_stage = leave_csv_cleanup_debt(
        base, "marker-csv-format-tamper", csv_ticker)
    csv_marker_path = csv_stage / storage.ORDINARY_CORRECTION_TXN_MARKER
    csv_marker_raw = csv_marker_path.read_bytes()
    csv_marker = json.loads(csv_marker_raw.decode("utf-8"))
    csv_marker["old_rel"] = csv_marker["write_rel"]
    storage._atomic_write_bytes(
        csv_marker_path, json.dumps(
            csv_marker, sort_keys=True, separators=(",", ":")).encode(
                "utf-8"))

    csv_scan = storage.scan_storage(csv_root, workers=1)
    csv_parquet = storage.month_file_path(
        csv_root, csv_ticker, 2024, 6, INTERVAL)
    check("CSV-origin marker cannot claim the canonical Parquet prestate",
          csv_path.exists() and csv_parquet.exists() and csv_stage.exists(),
          str(csv_scan))
    check("CSV-to-Parquet format tamper retains explicit recovery debt",
          bool(csv_scan["errors"]), str(csv_scan))

    storage._atomic_write_bytes(csv_marker_path, csv_marker_raw)
    storage.scan_storage(csv_root, workers=1)
    csv_ok, csv_detail = coherent(csv_root, csv_ticker, "new")
    check("restored CSV-origin marker retires its twin and recovers",
          csv_ok and not csv_path.exists(), csv_detail)

    parquet_ticker = "PARFMTWAL"
    parquet_root, parquet_path, parquet_stage = leave_parquet_cleanup_debt(
        base, "marker-parquet-format-tamper", parquet_ticker)
    parquet_marker_path = (
        parquet_stage / storage.ORDINARY_CORRECTION_TXN_MARKER)
    parquet_marker_raw = parquet_marker_path.read_bytes()
    parquet_marker = json.loads(parquet_marker_raw.decode("utf-8"))
    canonical_csv = storage.month_file_path(
        parquet_root, parquet_ticker, 2024, 6, INTERVAL, fmt="csv")
    parquet_marker["old_rel"] = canonical_csv.relative_to(
        parquet_root / parquet_ticker).as_posix()
    storage._atomic_write_bytes(
        parquet_marker_path, json.dumps(
            parquet_marker, sort_keys=True, separators=(",", ":")).encode(
                "utf-8"))

    parquet_scan = storage.scan_storage(parquet_root, workers=1)
    check("Parquet-origin marker cannot claim the canonical CSV prestate",
          parquet_path.exists() and not canonical_csv.exists()
          and parquet_stage.exists(), str(parquet_scan))
    check("Parquet-to-CSV format tamper retains explicit recovery debt",
          bool(parquet_scan["errors"]), str(parquet_scan))

    storage._atomic_write_bytes(parquet_marker_path, parquet_marker_raw)
    storage.scan_storage(parquet_root, workers=1)
    parquet_ok, parquet_detail = coherent(
        parquet_root, parquet_ticker, "new")
    check("restored Parquet-origin marker recovers normally",
          parquet_ok, parquet_detail)


def changed_csv_pending_debt(base):
    ticker = "CHGWAL"
    root, csv_path, stage = leave_csv_cleanup_debt(
        base, "changed-csv-debt", ticker)
    original = csv_path.read_bytes()
    changed = b"foreign changed CSV bytes\r\n"
    storage._atomic_write_bytes(csv_path, changed)

    scan = storage.scan_storage(root, workers=1)
    check("changed legacy CSV is never deleted during WAL recovery",
          csv_path.read_bytes() == changed and stage.exists(), str(scan))
    check("changed legacy CSV retains explicit recovery debt",
          bool(scan["errors"]), str(scan))

    storage._atomic_write_bytes(csv_path, original)
    storage.scan_storage(root, workers=1)
    ok, detail = coherent(root, ticker, "new")
    check("restored exact legacy CSV recovers and preserves ledger", ok,
          detail)


def main():
    with tempfile.TemporaryDirectory(
            prefix="ordinary-correction-wal-") as temp:
        hard_exit_matrix(Path(temp))
        for writer in ("ibkr", "ingest"):
            manifest_failure(temp, writer)
            csv_unlink_failure(temp, writer)
        marker_path_tampering(temp)
        marker_format_tampering(temp)
        changed_csv_pending_debt(temp)
    print(f"\n{CHECKS}/{CHECKS} checks passed" if not FAILURES else
          f"\n{CHECKS - len(FAILURES)}/{CHECKS} checks passed")
    if FAILURES:
        print("Failures: " + ", ".join(FAILURES))
        raise SystemExit(1)


if __name__ == "__main__":
    if len(sys.argv) == 6 and sys.argv[1] == "--child":
        child_main(sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
    main()
