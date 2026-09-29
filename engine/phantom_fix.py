"""Apply externally verified phantom-split corrections to stored bars.

This is intentionally narrow. A phantom correction is only allowed after the
triage classifier returns PHANTOM for the supplied ticker/ex-date. It rewrites
only affected OHLC month files, snapshots the originals first, and records a
durable manifest note so future refetches can be audited.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import shutil
import sys
from pathlib import Path

import stock_storage as ss
import triage_classifier


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_SNAPSHOT_ROOT = PROJECT_ROOT / "archive" / "_code_snapshots"
DEFAULT_TICKER = "FTNT"
DEFAULT_EX_DATE = dt.date(2014, 1, 13)
DEFAULT_FACTOR = 4
DEFAULT_INTERVALS = ("1d", "1m")
CORRECTION_TYPE = "phantom_split_correction"


def _parse_date(value):
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    return dt.date.fromisoformat(str(value)[:10])


def _month_entry(manifest, interval, month):
    return (((manifest.get("intervals") or {}).get(interval) or {})
            .get("months") or {}).get(month) or {}


def _load_month(root, ticker, interval, month):
    year, mon = int(month[:4]), int(month[5:7])
    path = (ss.find_month_file(root, ticker, year, mon, interval)
            or ss.month_file_path(root, ticker, year, mon, interval))
    bars, stats = ss.read_month_file(path)
    return path, bars, stats


def _correct_bars(bars, ex_date, factor):
    corrected = []
    changed = 0
    for ts, opn, high, low, close, volume in bars:
        if ts.date() < ex_date:
            corrected.append((
                ts,
                float(opn) * factor,
                float(high) * factor,
                float(low) * factor,
                float(close) * factor,
                int(round(int(volume) / factor)),
            ))
            changed += 1
        else:
            corrected.append((ts, opn, high, low, close, volume))
    return corrected, changed


def _would_change_file(root, ticker, interval, month, ex_date):
    try:
        path, bars, stats = _load_month(root, ticker, interval, month)
    except Exception as exc:  # noqa: BLE001
        return {"interval": interval, "month": month, "error": str(exc)}
    n = sum(1 for bar in bars if bar[0].date() < ex_date)
    if not n:
        return None
    return {
        "interval": interval,
        "month": month,
        "path": str(path),
        "bars": n,
        "rows": len(bars),
        "before_sha256": stats.get("sha256"),
    }


def plan(root, ticker=DEFAULT_TICKER, ex_date=DEFAULT_EX_DATE,
         intervals=DEFAULT_INTERVALS):
    """Return affected month files without writing."""
    root = Path(root)
    ticker = ss.canonical_ticker(ticker)
    ex_date = _parse_date(ex_date)
    manifest = ss.load_manifest(root / ticker) or {}
    out = []
    for interval in intervals:
        for month in sorted(ss.manifest_months(manifest, interval)):
            item = _would_change_file(root, ticker, interval, month, ex_date)
            if item:
                out.append(item)
    return out


def _snapshot_files(items, snapshot_root, storage_root=None):
    snapshot_root = Path(snapshot_root)
    bases = []
    if storage_root is not None:
        storage_root = Path(storage_root).resolve()
        bases.extend([storage_root.parent, storage_root])
    bases.append(Path.cwd().resolve())
    copied = []
    for item in items:
        if item.get("error"):
            continue
        src = Path(item["path"]).resolve()
        rel = None
        for base in bases:
            try:
                rel = src.relative_to(base)
                break
            except ValueError:
                continue
        if rel is None:
            rel = Path(src.name)
        dst = snapshot_root / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
        copied.append(str(dst))
    return copied


def _manifest_note(ticker, ex_date, factor, intervals, applied, snapshot_root,
                   triage):
    return {
        "type": CORRECTION_TYPE,
        "ticker": ticker,
        "ex_date": ex_date.isoformat(),
        "factor": factor,
        "price_operation": f"OHLC *= {factor} for dates before ex_date",
        "volume_operation": f"volume = round(volume / {factor}) for dates "
                            "before ex_date",
        "intervals": list(intervals),
        "verified_vs": "stockanalysis external anchors via Claude reference; "
                       "triage_classifier verdict PHANTOM",
        "applied": applied,
        "snapshot": str(snapshot_root),
        "triage_evidence": triage.get("evidence") if isinstance(triage, dict)
                           else None,
    }


def _record_manifest(manifest, note):
    corrections = manifest.setdefault("data_corrections", [])
    for existing in corrections:
        if (existing.get("type") == note["type"]
                and existing.get("ex_date") == note["ex_date"]
                and int(existing.get("factor") or 0) == int(note["factor"])):
            existing.update(note)
            return
    corrections.append(note)


def apply_correction(root, ticker=DEFAULT_TICKER, ex_date=DEFAULT_EX_DATE,
                     factor=DEFAULT_FACTOR, intervals=DEFAULT_INTERVALS,
                     snapshot_root=None, dry_run=False, asof=None,
                     ref_fn=None):
    """Apply a confirmed phantom correction.

    ``dry_run=True`` plans and runs the classifier gate but performs no writes.
    """
    root = Path(root)
    ticker = ss.canonical_ticker(ticker)
    ex_date = _parse_date(ex_date)
    factor = int(factor)
    intervals = tuple(intervals)
    asof = asof or dt.datetime.now().isoformat(timespec="seconds")

    triage = triage_classifier.classify_flag(
        root, ticker, ex_date, ref_fn=ref_fn)
    if triage.get("verdict") != "PHANTOM":
        raise RuntimeError(
            f"refusing correction: triage verdict is {triage.get('verdict')}, "
            "not PHANTOM")

    items = plan(root, ticker=ticker, ex_date=ex_date, intervals=intervals)
    errors = [item for item in items if item.get("error")]
    if errors:
        raise RuntimeError(f"cannot read affected months: {errors[:3]}")
    total_bars = sum(int(item.get("bars") or 0) for item in items)
    if dry_run:
        return {
            "ticker": ticker,
            "ex_date": ex_date.isoformat(),
            "factor": factor,
            "intervals": list(intervals),
            "dry_run": True,
            "affected_files": items,
            "affected_file_count": len(items),
            "affected_bars": total_bars,
            "triage": triage,
        }

    if snapshot_root is None:
        stamp = asof.replace(":", "").replace("-", "")[:15]
        snapshot_root = (DEFAULT_SNAPSHOT_ROOT
                         / f"{stamp}_ftnt_phantom_fix")
    snapshot_root = Path(snapshot_root)
    copied = _snapshot_files(items, snapshot_root, root)
    if len(copied) != len(items):
        raise RuntimeError(
            f"snapshot incomplete: copied {len(copied)} of {len(items)} files")

    manifest_path = root / ticker
    manifest = ss.load_manifest(manifest_path)
    if not manifest:
        raise RuntimeError(f"manifest unavailable for {ticker}")

    rewritten = []
    for item in items:
        interval = item["interval"]
        month = item["month"]
        path, bars, _stats = _load_month(root, ticker, interval, month)
        corrected, changed = _correct_bars(bars, ex_date, factor)
        if changed != item["bars"]:
            raise RuntimeError(
                f"{ticker} {interval} {month}: changed-row count drifted "
                f"from {item['bars']} to {changed}")
        stats = ss.write_month_file(path, corrected, verify_after_write=True)
        entry = _month_entry(manifest, interval, month)
        source = entry.get("source")
        status = entry.get("status", "present")
        entry.update(stats)
        entry["status"] = status
        if source is not None:
            entry["source"] = source
        entry.setdefault("corrections", []).append({
            "type": CORRECTION_TYPE,
            "ex_date": ex_date.isoformat(),
            "factor": factor,
            "applied": asof,
            "bars": changed,
        })
        rewritten.append({
            **item,
            "after_sha256": stats.get("sha256"),
            "after_size": stats.get("size"),
        })

    note = _manifest_note(
        ticker, ex_date, factor, intervals, asof, snapshot_root, triage)
    _record_manifest(manifest, note)
    ss.save_manifest(manifest_path, manifest)

    return {
        "ticker": ticker,
        "ex_date": ex_date.isoformat(),
        "factor": factor,
        "intervals": list(intervals),
        "dry_run": False,
        "snapshot_root": str(snapshot_root),
        "snapshot_files": copied,
        "affected_files": rewritten,
        "affected_file_count": len(rewritten),
        "affected_bars": total_bars,
        "manifest_note": note,
        "triage": triage,
    }


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("root", nargs="?", default=str(ss.storage_root(".")))
    parser.add_argument("--ticker", default=DEFAULT_TICKER)
    parser.add_argument("--ex-date", default=DEFAULT_EX_DATE.isoformat())
    parser.add_argument("--factor", type=int, default=DEFAULT_FACTOR)
    parser.add_argument("--intervals", default=",".join(DEFAULT_INTERVALS))
    parser.add_argument("--snapshot-root")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--json")
    args = parser.parse_args(sys.argv[1:] if argv is None else argv)

    result = apply_correction(
        args.root,
        ticker=args.ticker,
        ex_date=args.ex_date,
        factor=args.factor,
        intervals=tuple(x.strip() for x in args.intervals.split(",")
                        if x.strip()),
        snapshot_root=args.snapshot_root,
        dry_run=args.dry_run,
    )
    text = json.dumps(result, indent=2, sort_keys=True)
    if args.json:
        Path(args.json).write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
