#!/usr/bin/env python3
"""Create the committed, read-only bank evidence input for Row 79's calendar.

This tool never writes under ``Stock Data Storage``.  It validates the trusted-head
and consensus sidecars, verifies that the consensus cache still matches every live
daily manifest, and reduces regular-session 1-minute parquet data to the minimum
close-time fact the stdlib schedule generator needs: for each date, the second-latest
nonzero-volume timestamp across distinct tickers.  Two tickers at or after a claimed
close convict that close; zero-volume phantom bars are deliberately ignored.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import sys
import tempfile
from collections import Counter
from datetime import date, datetime
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_BANK_ROOT = PROJECT_ROOT / "Stock Data Storage"
DEFAULT_OUTPUT = Path(__file__).with_name("session_schedule_evidence.json")
EARLIEST_NAME = "_ibkr_earliest.json"
CONSENSUS_NAME = "_consensus_calendar.json"
SPECIAL_NAME = "_special_closures.json"
EPOCH_ORDINAL = date(1970, 1, 1).toordinal()


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("ascii")


def _digest_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _digest_value(value: object) -> str:
    return _digest_bytes(_canonical_bytes(value))


def _load_json(path: Path, label: str) -> tuple[object, bytes]:
    try:
        raw = path.read_bytes()
        return json.loads(raw.decode("utf-8")), raw
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"{label}: cannot read {path}: {exc}") from exc


def _iso_date(value: object, label: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{label}: expected canonical ISO date string")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{label}: invalid date {value!r}") from exc
    if parsed.isoformat() != value:
        raise ValueError(f"{label}: date is not canonical ISO format")
    return value


def _snapshot_earliest(bank_root: Path) -> dict:
    path = bank_root / EARLIEST_NAME
    payload, raw = _load_json(path, "trusted-head sidecar")
    if not isinstance(payload, dict) or not payload:
        raise ValueError("trusted-head sidecar must be a nonempty object")

    scalar_count = 0
    object_count = 0
    parsed: list[tuple[str, str]] = []
    for ticker, value in sorted(payload.items()):
        if not isinstance(ticker, str) or not ticker or ticker != ticker.upper():
            raise ValueError(f"trusted-head key is not canonical: {ticker!r}")
        if isinstance(value, str):
            earliest = _iso_date(value, f"trusted-head {ticker}")
            scalar_count += 1
        elif isinstance(value, dict):
            if set(value) != {"conid", "earliest"}:
                raise ValueError(
                    f"trusted-head {ticker}: object keys must be conid/earliest")
            conid = value["conid"]
            if isinstance(conid, bool) or not isinstance(conid, int) or conid <= 0:
                raise ValueError(f"trusted-head {ticker}: conid must be positive int")
            earliest = _iso_date(
                value["earliest"], f"trusted-head {ticker}.earliest")
            object_count += 1
        else:
            raise ValueError(
                f"trusted-head {ticker}: expected scalar or identity object")
        parsed.append((ticker, earliest))

    oldest = min(value for _ticker, value in parsed)
    oldest_tickers = sorted(ticker for ticker, value in parsed if value == oldest)
    return {
        "state": "present",
        "source": EARLIEST_NAME,
        "source_sha256": _digest_bytes(raw),
        "entry_count": len(parsed),
        "shape_counts": {"identity_object": object_count, "scalar": scalar_count},
        "oldest_date": oldest,
        "oldest_tickers": oldest_tickers,
        "parsed_entries_sha256": _digest_value(parsed),
    }


def _special_dates(payload: object) -> list[str]:
    if not isinstance(payload, dict):
        raise ValueError("special-closure sidecar must be an object")
    if not ({"closures", "dates"} & set(payload)):
        raise ValueError("special-closure sidecar has no closure list")
    raw = payload.get("closures", payload.get("dates", []))
    if isinstance(raw, dict):
        raw = list(raw.values())
    if not isinstance(raw, list):
        raise ValueError("special-closure list must be an array or object")
    values = []
    for index, item in enumerate(raw):
        if isinstance(item, dict):
            if "date" not in item:
                raise ValueError(f"special closure {index}: missing date")
            item = item["date"]
        values.append(_iso_date(item, f"special closure {index}"))
    if len(values) != len(set(values)):
        raise ValueError("special-closure sidecar repeats a date")
    return sorted(values)


def _snapshot_special(bank_root: Path) -> dict:
    path = bank_root / SPECIAL_NAME
    if not path.exists():
        canonical = {"state": "absent", "dates": []}
        return {
            **canonical,
            "source": SPECIAL_NAME,
            "source_sha256": None,
            "canonical_sha256": _digest_value(canonical),
        }
    payload, raw = _load_json(path, "special-closure sidecar")
    dates = _special_dates(payload)
    state = "empty" if not dates else "present"
    canonical = {"state": state, "dates": dates}
    return {
        **canonical,
        "source": SPECIAL_NAME,
        "source_sha256": _digest_bytes(raw),
        "canonical_sha256": _digest_value(canonical),
    }


def _calendar_fingerprint(months: dict, label: str) -> str:
    if not isinstance(months, dict):
        raise ValueError(f"{label}: months must be an object")
    digest = hashlib.sha256()
    for month in sorted(months):
        entry = months[month]
        if not isinstance(entry, dict):
            raise ValueError(f"{label}.{month}: month entry must be an object")
        digest.update(
            f"{month}:{entry.get('sha256')}:{entry.get('mtime_ns')}:"
            f"{entry.get('rows')}\n".encode("utf-8"))
    return digest.hexdigest()


def _live_daily_manifest_keys(bank_root: Path) -> dict[str, str]:
    result = {}
    for ticker_dir in sorted(
            (item for item in bank_root.iterdir() if item.is_dir()),
            key=lambda item: item.name):
        manifest_path = ticker_dir / "manifest.json"
        if not manifest_path.is_file():
            continue
        payload, _raw = _load_json(manifest_path, f"manifest {ticker_dir.name}")
        if not isinstance(payload, dict):
            raise ValueError(f"manifest {ticker_dir.name}: root must be object")
        intervals = payload.get("intervals")
        if not isinstance(intervals, dict):
            continue
        daily = intervals.get("1d")
        if not isinstance(daily, dict):
            continue
        months = daily.get("months")
        if not isinstance(months, dict) or not months:
            continue
        symbol = payload.get("symbol", ticker_dir.name)
        if not isinstance(symbol, str) or not symbol:
            raise ValueError(f"manifest {ticker_dir.name}: invalid symbol")
        key = f"{symbol.upper()} 1d"
        if key in result:
            raise ValueError(f"duplicate live daily manifest key {key}")
        result[key] = _calendar_fingerprint(months, key)
    return result


def _snapshot_consensus(bank_root: Path) -> dict:
    path = bank_root / CONSENSUS_NAME
    payload, raw = _load_json(path, "consensus sidecar")
    if not isinstance(payload, dict) or set(payload) != {"tickers"}:
        raise ValueError("consensus sidecar must contain only tickers")
    tickers = payload["tickers"]
    if not isinstance(tickers, dict):
        raise ValueError("consensus tickers must be an object")

    live = _live_daily_manifest_keys(bank_root)
    if set(tickers) != set(live):
        missing = sorted(set(live) - set(tickers))[:10]
        stale = sorted(set(tickers) - set(live))[:10]
        raise ValueError(
            f"consensus cache/live manifest key drift: missing={missing}, stale={stale}")

    counts: Counter[int] = Counter()
    for key in sorted(tickers):
        entry = tickers[key]
        if not isinstance(entry, dict) or set(entry) != {"days", "key"}:
            raise ValueError(f"consensus {key}: expected days/key object")
        if entry["key"] != live[key]:
            raise ValueError(f"consensus {key}: manifest fingerprint is stale")
        days = entry["days"]
        if (not isinstance(days, list)
                or any(isinstance(value, bool) or not isinstance(value, int)
                       for value in days)
                or days != sorted(set(days))):
            raise ValueError(f"consensus {key}: days must be sorted unique integers")
        counts.update(days)

    positive = [
        date.fromordinal(EPOCH_ORDINAL + value).isoformat()
        for value, count in sorted(counts.items()) if count >= 2
    ]
    if not positive:
        raise ValueError("consensus evidence has no two-ticker dates")
    return {
        "state": "present",
        "source": CONSENSUS_NAME,
        "source_sha256": _digest_bytes(raw),
        "live_manifest_set_sha256": _digest_value(sorted(live.items())),
        "series_count": len(tickers),
        "min_tickers": 2,
        "positive_date_count": len(positive),
        "first_positive_date": positive[0],
        "last_positive_date": positive[-1],
        "positive_dates": positive,
        "positive_dates_sha256": _digest_value(positive),
    }


def _nonzero_daily_final_minutes(
        timestamps: list, volumes: list, relative: str
) -> dict[str, int]:
    if len(timestamps) != len(volumes):
        raise ValueError(f"1m evidence {relative}: ts/volume length mismatch")
    result: dict[str, int] = {}
    for index, (stamp, volume) in enumerate(zip(timestamps, volumes)):
        if not isinstance(stamp, datetime) or stamp.tzinfo is not None:
            raise ValueError(
                f"1m evidence {relative}[{index}]: expected NY-naive datetime")
        if stamp.second or stamp.microsecond:
            raise ValueError(
                f"1m evidence {relative}[{index}]: timestamp is not minute-aligned")
        if (isinstance(volume, bool)
                or not isinstance(volume, (int, float))
                or not math.isfinite(float(volume))):
            raise ValueError(f"1m evidence {relative}[{index}]: invalid volume")
        if float(volume) <= 0.0:
            continue
        day = stamp.date().isoformat()
        minute = stamp.hour * 60 + stamp.minute
        if minute > result.get(day, -1):
            result[day] = minute
    return result


def _snapshot_close_times(bank_root: Path) -> dict:
    try:
        import pyarrow.parquet as parquet
    except ImportError as exc:
        raise ValueError(
            "close-time snapshot requires the project runtime's pyarrow") from exc

    finals_by_day: dict[str, dict[int, list[str]]] = {}
    file_descriptors = []
    file_count = 0
    ticker_count = 0
    for ticker_dir in sorted(
            (item for item in bank_root.iterdir() if item.is_dir()),
            key=lambda item: item.name):
        ticker_daily: dict[str, int] = {}
        files = sorted(ticker_dir.rglob("*_1m.parquet"))
        if not files:
            continue
        ticker = ticker_dir.name
        if not ticker or ticker != ticker.upper() or "," in ticker:
            raise ValueError(f"1m evidence ticker is not canonical: {ticker!r}")
        for path in files:
            relative = path.relative_to(bank_root).as_posix()
            try:
                table = parquet.ParquetFile(path).read(columns=["ts", "volume"])
                columns = table.to_pydict()
            except Exception as exc:
                raise ValueError(f"cannot read 1m evidence {relative}: {exc}") from exc
            timestamps = columns.get("ts")
            volumes = columns.get("volume")
            if not isinstance(timestamps, list) or not isinstance(volumes, list) \
                    or len(timestamps) != len(volumes):
                raise ValueError(f"1m evidence {relative}: malformed ts/volume columns")
            for day, minute in _nonzero_daily_final_minutes(
                    timestamps, volumes, relative).items():
                if minute > ticker_daily.get(day, -1):
                    ticker_daily[day] = minute
            stat = path.stat()
            file_descriptors.append((relative, stat.st_size))
            file_count += 1

        for day, minute in ticker_daily.items():
            finals_by_day.setdefault(day, {}).setdefault(minute, []).append(ticker)
        ticker_count += 1
        if ticker_count % 25 == 0:
            print(
                f"close-time snapshot: {ticker_count} ticker directories, "
                f"{file_count} parquet files read",
                file=sys.stderr,
                flush=True,
            )

    rows = []
    for day, groups in sorted(finals_by_day.items()):
        count = sum(len(tickers) for tickers in groups.values())
        if count < 1:
            continue
        encoded_groups = {
            f"{minute // 60:02d}:{minute % 60:02d}": ",".join(sorted(tickers))
            for minute, tickers in sorted(groups.items())
        }
        rows.append({
            "date": day,
            "final_nonzero_tickers_by_et": encoded_groups,
            "ticker_count_with_nonzero": count,
        })
    if not rows:
        raise ValueError("1m evidence has no nonzero-volume dates")
    return {
        "state": "present",
        "source_interval": "1m",
        "semantics": (
            "per-ticker final nonzero-volume NY-naive minute grouped by ET; "
            "group values are comma-separated canonical ticker names; zero-volume "
            "phantom bars ignored"),
        "source_file_count": file_count,
        "source_descriptor_sha256": _digest_value(file_descriptors),
        "date_count": len(rows),
        "first_date": rows[0]["date"],
        "last_date": rows[-1]["date"],
        "dates": rows,
        "dates_sha256": _digest_value(rows),
    }


def build_snapshot(bank_root: Path, captured_as_of: str) -> dict:
    captured_as_of = _iso_date(captured_as_of, "captured_as_of")
    bank_root = bank_root.resolve(strict=True)
    if not bank_root.is_dir():
        raise ValueError("bank root is not a directory")
    result = {
        "schema_version": 1,
        "captured_as_of": captured_as_of,
        "bank_root_label": "Stock Data Storage",
        "read_only_contract": (
            "snapshot reads bank evidence only; it never creates or modifies a bank file"),
        "trusted_heads": _snapshot_earliest(bank_root),
        "special_closure_sidecar": _snapshot_special(bank_root),
        "consensus": _snapshot_consensus(bank_root),
        "close_time_evidence": _snapshot_close_times(bank_root),
    }
    return result


def _write_output(path: Path, payload: dict, bank_root: Path) -> None:
    path = path.resolve()
    bank_root = bank_root.resolve()
    try:
        path.relative_to(bank_root)
    except ValueError:
        pass
    else:
        raise ValueError("evidence output must not be inside Stock Data Storage")
    path.parent.mkdir(parents=True, exist_ok=True)
    rendered = json.dumps(
        payload, indent=2, sort_keys=True, ensure_ascii=True).encode("ascii") + b"\n"
    fd, temp_name = tempfile.mkstemp(
        prefix=path.name + ".", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(rendered)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, path)
    except BaseException:
        try:
            os.unlink(temp_name)
        except OSError:
            pass
        raise


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bank-root", type=Path, default=DEFAULT_BANK_ROOT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--as-of", required=True)
    args = parser.parse_args(argv)
    try:
        payload = build_snapshot(args.bank_root, args.as_of)
        _write_output(args.output, payload, args.bank_root)
    except ValueError as exc:
        parser.exit(2, f"evidence snapshot failed: {exc}\n")
    report = {
        "status": "ok",
        "output": str(args.output),
        "snapshot_sha256": _digest_value(payload),
        "trusted_head_count": payload["trusted_heads"]["entry_count"],
        "consensus_positive_dates": payload["consensus"]["positive_date_count"],
        "close_time_dates": payload["close_time_evidence"]["date_count"],
        "one_minute_files": payload["close_time_evidence"]["source_file_count"],
    }
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
