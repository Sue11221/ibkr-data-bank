"""Truncate predecessor history from a stored ticker identity.

The WBD bank series contains pre-merger Discovery history under the WBD symbol.
This tool keeps bars on or after 2022-04-11, snapshots every affected month and
the manifest, and records a durable identity_truncation correction.

If a durable correction note exists while pre-cutover bars remain, the state is
inconsistent. The tool refuses before triage or mutation. Restore the correction
snapshot, run ``verify_state()``, and then rerun the guarded operation; do not
recover by editing only the manifest note.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import shutil
import sys
from pathlib import Path

import stock_storage as ss
import triage_classifier


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_SNAPSHOT_ROOT = PROJECT_ROOT / "archive" / "_code_snapshots"
DEFAULT_TICKER = "WBD"
DEFAULT_BOUNDARY = dt.date(2021, 3, 30)
DEFAULT_CUTOVER = dt.date(2022, 4, 11)
DEFAULT_INTERVALS = ("1d", "1m", "1d-hvol")
CORRECTION_TYPE = "identity_truncation"
INCONSISTENT_CORRECTION_ERROR = (
    "existing identity_truncation correction is inconsistent: "
    "pre-cutover bars remain; restore the recorded snapshot before retrying")


def _parse_date(value):
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    return dt.date.fromisoformat(str(value)[:10])


def _sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_month(root, ticker, interval, month):
    year, mon = int(month[:4]), int(month[5:7])
    path = (ss.find_month_file(root, ticker, year, mon, interval)
            or ss.month_file_path(root, ticker, year, mon, interval))
    bars, stats = ss.read_month_file(path)
    return path, bars, stats


def _present_months(manifest, interval):
    months = ss.manifest_months(manifest, interval)
    return {
        str(month): entry
        for month, entry in months.items()
        if str((entry or {}).get("status") or "present").lower() == "present"
    }


def _existing_note(manifest, ticker, cutover):
    wanted = cutover.isoformat()
    for note in manifest.get("data_corrections") or []:
        if not isinstance(note, dict):
            continue
        if (note.get("type") == CORRECTION_TYPE
                and str(note.get("ticker") or ticker).upper() == ticker
                and str(note.get("cutover") or "")[:10] == wanted):
            return note
    return None


def plan(root, ticker=DEFAULT_TICKER, cutover=DEFAULT_CUTOVER,
         intervals=DEFAULT_INTERVALS):
    """Return every manifest month containing bars before ``cutover``."""
    root = Path(root)
    ticker = ss.canonical_ticker(ticker)
    cutover = _parse_date(cutover)
    manifest = ss.load_manifest(root / ticker)
    if not manifest:
        raise RuntimeError(f"manifest unavailable for {ticker}")

    items = []
    for interval in tuple(intervals):
        for month in sorted(_present_months(manifest, interval)):
            try:
                path, bars, stats = _load_month(
                    root, ticker, interval, month)
            except Exception as exc:  # noqa: BLE001
                items.append({
                    "interval": interval,
                    "month": month,
                    "error": str(exc),
                })
                continue
            dropped = sum(1 for bar in bars if bar[0].date() < cutover)
            if not dropped:
                continue
            kept = len(bars) - dropped
            items.append({
                "interval": interval,
                "month": month,
                "path": str(path),
                "action": "remove" if kept == 0 else "rewrite",
                "dropped_bars": dropped,
                "kept_bars": kept,
                "before_rows": len(bars),
                "before_sha256": stats.get("sha256") or _sha256(path),
                "before_size": stats.get("size"),
            })
    return items


def _item_signature(items):
    return [
        (
            item.get("interval"),
            item.get("month"),
            item.get("action"),
            item.get("dropped_bars"),
            item.get("kept_bars"),
            item.get("before_sha256"),
            item.get("error"),
        )
        for item in items
    ]


def _snapshot_files(items, manifest_path, snapshot_root, storage_root):
    snapshot_root = Path(snapshot_root)
    if snapshot_root.exists():
        raise RuntimeError(f"snapshot path already exists: {snapshot_root}")
    snapshot_root.mkdir(parents=True)

    storage_root = Path(storage_root).resolve()
    base = storage_root.parent
    sources = [
        (Path(item["path"]).resolve(), item["before_sha256"], "month")
        for item in items
    ]
    sources.append((Path(manifest_path).resolve(), _sha256(manifest_path),
                    "manifest"))

    copied = []
    for src, expected_sha, kind in sources:
        try:
            relative = src.relative_to(base)
        except ValueError:
            relative = Path(src.name)
        dst = snapshot_root / relative
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
        copied_sha = _sha256(dst)
        if copied_sha != expected_sha:
            raise RuntimeError(
                f"snapshot hash mismatch for {src}: {copied_sha} != "
                f"{expected_sha}")
        copied.append({
            "kind": kind,
            "source": str(src),
            "snapshot": str(dst),
            "sha256": copied_sha,
        })
    if len(copied) != len(items) + 1:
        raise RuntimeError(
            f"snapshot incomplete: copied {len(copied)} of {len(items) + 1} "
            "required files")
    return copied


def _restore_snapshot(copied):
    restored = []
    errors = []
    for record in copied:
        src = Path(record["snapshot"])
        dst = Path(record["source"])
        tmp = dst.with_name(f".{dst.name}.restore-{os.getpid()}.tmp")
        try:
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, tmp)
            os.replace(tmp, dst)
            if _sha256(dst) != record["sha256"]:
                raise RuntimeError("restored hash does not match snapshot")
            restored.append(str(dst))
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{dst}: {exc}")
            try:
                tmp.unlink(missing_ok=True)
            except OSError:
                pass
    return {"restored": restored, "errors": errors}


def _truncate_bars(bars, cutover):
    kept = [bar for bar in bars if bar[0].date() >= cutover]
    return kept, len(bars) - len(kept)


def _trim_verified_absent(manifest, intervals, cutover):
    removed = {}
    for interval in intervals:
        section = (manifest.setdefault("intervals", {})
                           .setdefault(interval, {"months": {},
                                                  "verified_absent": []}))
        current = list(section.get("verified_absent") or [])
        keep = []
        removed_days = []
        for value in current:
            try:
                is_old = _parse_date(value) < cutover
            except (TypeError, ValueError):
                is_old = False
            if is_old:
                removed_days.append(str(value))
            else:
                keep.append(value)
        section["verified_absent"] = sorted(set(keep))
        removed[interval] = len(removed_days)
    return removed


def _prune_empty_dirs(ticker_dir):
    """Remove empty month/year directories below a ticker, deepest first."""
    ticker_dir = Path(ticker_dir).resolve()
    removed = []
    directories = sorted(
        (path for path in ticker_dir.rglob("*") if path.is_dir()),
        key=lambda path: len(path.parts),
        reverse=True,
    )
    for path in directories:
        try:
            path.rmdir()
        except OSError:
            continue
        removed.append(str(path))
    return removed


def _manifest_note(ticker, boundary, cutover, intervals, items,
                   snapshot_root, triage, applied):
    by_interval = {}
    for item in items:
        by_interval[item["interval"]] = (
            by_interval.get(item["interval"], 0)
            + int(item["dropped_bars"]))
    return {
        "type": CORRECTION_TYPE,
        "ticker": ticker,
        "boundary": boundary.isoformat(),
        "cutover": cutover.isoformat(),
        "intervals": list(intervals),
        "dropped_bars": sum(by_interval.values()),
        "dropped_bars_by_interval": by_interval,
        "affected_month_files": len(items),
        "removed_month_files": sum(
            item["action"] == "remove" for item in items),
        "rewritten_month_files": sum(
            item["action"] == "rewrite" for item in items),
        "applied": applied,
        "snapshot": str(snapshot_root),
        "verified_vs": (
            "wbd_truncate_reference.py external daily anchors; pre-write "
            "triage verdict IDENTITY_BASIS"),
        "triage_evidence": triage.get("evidence"),
        "reason": (
            "pre-cutover bars belong to the Discovery predecessor identity; "
            "WBD began trading at the cutover"),
    }


def _record_manifest_note(manifest, note):
    corrections = manifest.setdefault("data_corrections", [])
    for current in corrections:
        if (isinstance(current, dict)
                and current.get("type") == note["type"]
                and str(current.get("cutover") or "")[:10]
                == note["cutover"]):
            current.update(note)
            return
    corrections.append(note)


def verify_state(root, ticker=DEFAULT_TICKER, cutover=DEFAULT_CUTOVER,
                 intervals=DEFAULT_INTERVALS):
    root = Path(root)
    ticker = ss.canonical_ticker(ticker)
    cutover = _parse_date(cutover)
    manifest = ss.load_manifest(root / ticker)
    errors = []
    first_dates = {}
    if not manifest:
        return {"ok": False, "errors": ["manifest unavailable"]}

    for interval in intervals:
        months = _present_months(manifest, interval)
        old_months = [month for month in months if month < cutover.isoformat()[:7]]
        if old_months:
            errors.append(f"{interval}: pre-cutover manifest months remain")
        if not months:
            errors.append(f"{interval}: no present months remain")
            continue
        first_month = sorted(months)[0]
        try:
            _path, bars, _stats = _load_month(
                root, ticker, interval, first_month)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{interval}: cannot read first month: {exc}")
            continue
        first = min(bar[0].date() for bar in bars)
        first_dates[interval] = first.isoformat()
        if first < cutover:
            errors.append(
                f"{interval}: first bar {first} is before {cutover}")
        old_absent = [
            value for value in (((manifest.get("intervals") or {})
                                  .get(interval) or {})
                                 .get("verified_absent") or [])
            if str(value)[:10] < cutover.isoformat()
        ]
        if old_absent:
            errors.append(
                f"{interval}: {len(old_absent)} pre-cutover verified-absent "
                "dates remain")

    note = _existing_note(manifest, ticker, cutover)
    if note is None:
        errors.append("identity_truncation manifest note missing")
    return {
        "ok": not errors,
        "ticker": ticker,
        "cutover": cutover.isoformat(),
        "first_dates": first_dates,
        "manifest_note": note,
        "errors": errors,
    }


def apply_truncation(root, ticker=DEFAULT_TICKER,
                     boundary=DEFAULT_BOUNDARY, cutover=DEFAULT_CUTOVER,
                     intervals=DEFAULT_INTERVALS, snapshot_root=None,
                     dry_run=True, asof=None, triage_fn=None, ref_fn=None):
    """Plan or apply the guarded identity truncation."""
    root = Path(root)
    ticker = ss.canonical_ticker(ticker)
    boundary = _parse_date(boundary)
    cutover = _parse_date(cutover)
    intervals = tuple(intervals)
    asof = asof or dt.datetime.now().astimezone().isoformat(timespec="seconds")
    manifest_path = root / ticker / ss.MANIFEST_NAME
    manifest = ss.load_manifest(root / ticker)
    if not manifest:
        raise RuntimeError(f"manifest unavailable for {ticker}")

    items = plan(root, ticker=ticker, cutover=cutover, intervals=intervals)
    errors = [item for item in items if item.get("error")]
    if errors:
        raise RuntimeError(f"cannot read affected months: {errors[:3]}")
    existing = _existing_note(manifest, ticker, cutover)
    if existing is not None and items:
        raise RuntimeError(INCONSISTENT_CORRECTION_ERROR)
    if not items:
        if existing is None:
            raise RuntimeError(
                "no pre-cutover bars found and no identity_truncation note exists")
        verification = verify_state(root, ticker, cutover, intervals)
        if not verification["ok"]:
            raise RuntimeError(
                f"existing truncation is inconsistent: {verification['errors']}")
        return {
            "ticker": ticker,
            "cutover": cutover.isoformat(),
            "dry_run": bool(dry_run),
            "already_applied": True,
            "affected_file_count": 0,
            "affected_bars": 0,
            "pruned_empty_dirs": (
                [] if dry_run else _prune_empty_dirs(root / ticker)),
            "verification": verification,
        }

    classify = triage_fn or triage_classifier.classify_flag
    triage = classify(root, ticker, boundary, ref_fn=ref_fn)
    if triage.get("verdict") != "IDENTITY_BASIS":
        raise RuntimeError(
            f"refusing truncation: triage verdict is "
            f"{triage.get('verdict')}, not IDENTITY_BASIS")

    total_bars = sum(int(item["dropped_bars"]) for item in items)
    by_interval = {}
    for item in items:
        by_interval[item["interval"]] = (
            by_interval.get(item["interval"], 0)
            + int(item["dropped_bars"]))
    base_result = {
        "ticker": ticker,
        "boundary": boundary.isoformat(),
        "cutover": cutover.isoformat(),
        "intervals": list(intervals),
        "affected_files": items,
        "affected_file_count": len(items),
        "affected_bars": total_bars,
        "affected_bars_by_interval": by_interval,
        "triage": triage,
    }
    if dry_run:
        return {**base_result, "dry_run": True, "already_applied": False}

    if snapshot_root is None:
        stamp = dt.datetime.now().strftime("%Y%m%dT%H%M%S")
        snapshot_root = DEFAULT_SNAPSHOT_ROOT / f"{stamp}_wbd_truncate"
    snapshot_root = Path(snapshot_root)
    manifest_sha = _sha256(manifest_path)
    copied = _snapshot_files(
        items, manifest_path, snapshot_root, root)

    if _sha256(manifest_path) != manifest_sha:
        raise RuntimeError("manifest changed while the snapshot was being made")
    fresh_items = plan(root, ticker=ticker, cutover=cutover,
                       intervals=intervals)
    if _item_signature(fresh_items) != _item_signature(items):
        raise RuntimeError("affected month files changed after planning")

    manifest = ss.load_manifest(root / ticker)
    applied_files = []
    mutated = False
    try:
        # Rewrite boundary months before deleting whole months. A codec failure
        # therefore occurs while every removable source file is still present.
        for item in [x for x in items if x["action"] == "rewrite"]:
            path, bars, _stats = _load_month(
                root, ticker, item["interval"], item["month"])
            if _sha256(path) != item["before_sha256"]:
                raise RuntimeError(f"source drift before rewrite: {path}")
            kept, dropped = _truncate_bars(bars, cutover)
            if dropped != item["dropped_bars"] or len(kept) != item["kept_bars"]:
                raise RuntimeError(
                    f"{item['interval']} {item['month']}: row counts drifted")
            if not kept:
                raise RuntimeError(
                    f"{item['interval']} {item['month']}: rewrite became empty")
            mutated = True
            stats = ss.write_month_file(path, kept, verify_after_write=True)
            entry = ss.manifest_months(
                manifest, item["interval"])[item["month"]]
            source = entry.get("source")
            corrections = list(entry.get("corrections") or [])
            entry.update(stats)
            entry["status"] = "present"
            if source is not None:
                entry["source"] = source
            corrections.append({
                "type": CORRECTION_TYPE,
                "cutover": cutover.isoformat(),
                "applied": asof,
                "dropped_bars": dropped,
            })
            entry["corrections"] = corrections
            applied_files.append({
                **item,
                "after_sha256": stats["sha256"],
                "after_rows": stats["rows"],
            })

        for item in [x for x in items if x["action"] == "remove"]:
            path = Path(item["path"])
            if _sha256(path) != item["before_sha256"]:
                raise RuntimeError(f"source drift before removal: {path}")
            mutated = True
            path.unlink()
            if path.exists():
                raise RuntimeError(f"file still exists after removal: {path}")
            ss.manifest_months(
                manifest, item["interval"]).pop(item["month"], None)
            applied_files.append({**item, "removed": True})

        trimmed_absent = _trim_verified_absent(
            manifest, intervals, cutover)
        note = _manifest_note(
            ticker, boundary, cutover, intervals, items, snapshot_root,
            triage, asof)
        note["trimmed_verified_absent"] = trimmed_absent
        _record_manifest_note(manifest, note)
        ss.save_manifest(root / ticker, manifest)
        pruned_empty_dirs = _prune_empty_dirs(root / ticker)

        verification = verify_state(root, ticker, cutover, intervals)
        removed_paths_ok = all(
            not Path(item["path"]).exists()
            for item in items if item["action"] == "remove")
        if not removed_paths_ok:
            verification["errors"].append("one or more removed paths remain")
            verification["ok"] = False
        if not verification["ok"]:
            raise RuntimeError(
                f"post-write verification failed: {verification['errors']}")
    except Exception as exc:
        rollback = _restore_snapshot(copied) if mutated else {
            "restored": [], "errors": []}
        if rollback["errors"]:
            raise RuntimeError(
                f"truncation failed ({exc}); rollback also failed: "
                f"{rollback['errors']}") from exc
        raise RuntimeError(
            f"truncation failed and was rolled back: {exc}") from exc

    return {
        **base_result,
        "dry_run": False,
        "already_applied": False,
        "applied": asof,
        "snapshot_root": str(snapshot_root),
        "snapshot_file_count": len(copied),
        "snapshot_files": [record["snapshot"] for record in copied],
        "applied_files": applied_files,
        "trimmed_verified_absent": trimmed_absent,
        "manifest_note": note,
        "pruned_empty_dirs": pruned_empty_dirs,
        "manifest_sha256_before": manifest_sha,
        "manifest_sha256_after": _sha256(manifest_path),
        "verification": verification,
    }


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("root", nargs="?", default=str(ss.storage_root(".")))
    parser.add_argument("--ticker", default=DEFAULT_TICKER)
    parser.add_argument("--boundary", default=DEFAULT_BOUNDARY.isoformat())
    parser.add_argument("--cutover", default=DEFAULT_CUTOVER.isoformat())
    parser.add_argument("--intervals", default=",".join(DEFAULT_INTERVALS))
    parser.add_argument("--snapshot-root")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--apply", action="store_true")
    parser.add_argument("--json")
    args = parser.parse_args(sys.argv[1:] if argv is None else argv)

    result = apply_truncation(
        args.root,
        ticker=args.ticker,
        boundary=args.boundary,
        cutover=args.cutover,
        intervals=tuple(
            value.strip() for value in args.intervals.split(",")
            if value.strip()),
        snapshot_root=args.snapshot_root,
        dry_run=not args.apply,
    )
    payload = json.dumps(result, indent=2, sort_keys=True)
    if args.json:
        Path(args.json).write_text(payload + "\n", encoding="utf-8")
    print(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
