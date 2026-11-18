"""Explicit date-only 1d replacement; never an ordinary conflict merge.

Work-in-progress implementation for offline review/development. Registration
and focused verification must finish before this can be an acceptance candidate.
No default bank, ticker, date, port or execution is implied.
"""

from __future__ import annotations

import base64
import argparse
from collections import Counter
from copy import deepcopy
from datetime import date, datetime, time
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
import uuid

import fetch_ibkr_bridge as bridge
import fetch_operations as fops
import fetch_operation_report as reports
import operation_gate
import stock_ibkr as ibkr
import stock_storage as storage


class DailyRepairError(RuntimeError):
    """A refusal leaves the original data, or an explicit recovery obligation."""


_KIND = "daily_date_repair_v1"
_MARKER = "transaction.json"
_PREPARING = "daily-preparing.json"
_MAX_WAL_BYTES = 32 * 1024 * 1024


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      allow_nan=False).encode("utf-8")


def _unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise DailyRepairError("duplicate JSON key")
        result[key] = value
    return result


def _decode(raw):
    def invalid_constant(value):
        raise DailyRepairError("nonfinite JSON value: " + value)
    try:
        return json.loads(raw, object_pairs_hook=_unique, parse_constant=invalid_constant)
    except (ValueError, UnicodeError) as exc:
        raise DailyRepairError("invalid transaction JSON") from exc


def _safe(root, path, *, exists=True):
    """Resolve inside the caller's bank, rejecting every reparse component."""
    root, path = Path(root).absolute(), Path(path).absolute()
    if not path.is_relative_to(root):
        raise DailyRepairError("path outside explicit bank")
    current = root
    for part in (None, *path.relative_to(root).parts):
        if part is not None:
            current = current / part
        if current.exists() or current.is_symlink():
            st = current.lstat()
            if current.is_symlink() or getattr(st, "st_file_attributes", 0) & 1024:
                raise DailyRepairError("reparse paths are not repair inputs")
    resolved = path.resolve(strict=exists)
    if not resolved.is_relative_to(root.resolve(strict=True)):
        raise DailyRepairError("resolved path escaped bank")
    return resolved


def explicit_pairs(pairs, interval="1d"):
    if interval != "1d" or not isinstance(pairs, (list, tuple)) or not pairs:
        raise DailyRepairError("explicit nonempty ticker/date pairs and 1d required")
    result = []
    for item in pairs:
        if not isinstance(item, dict) or set(item) != {"ticker", "date"}:
            raise DailyRepairError("pair must contain exactly ticker and date")
        ticker, stamp = item["ticker"], item["date"]
        if not isinstance(ticker, str) or storage.canonical_ticker(ticker) != ticker:
            raise DailyRepairError("ticker must use its exact canonical spelling")
        if not isinstance(stamp, str):
            raise DailyRepairError("date must be YYYY-MM-DD")
        try:
            day = date.fromisoformat(stamp)
        except ValueError as exc:
            raise DailyRepairError("invalid repair date") from exc
        if day.isoformat() != stamp or (ticker, day) in result:
            raise DailyRepairError("noncanonical or duplicate pair")
        result.append((ticker, day))
    return result


def _encoded_bar(bar):
    return [bar[0].isoformat(), *bar[1:]]


def _date_manifest(bars):
    result = {}
    for bar in bars:
        key = bar[0].date().isoformat()
        if key in result:
            raise DailyRepairError("daily month contains duplicate dates")
        result[key] = _sha(_json(_encoded_bar(bar)))
    return result


def _snapshot(root, ticker, day, *, pending=False):
    tdir = _safe(root, Path(root) / ticker)
    stage = tdir / storage.VOL_VALUE_RECONCILE_STAGE_DIR
    if not pending and (stage.exists() or stage.is_symlink()):
        raise DailyRepairError("pending correction requires explicit recovery")
    found = []
    for fmt in ("csv", "parquet"):
        p = storage.month_file_path(root, ticker, day.year, day.month, "1d", fmt=fmt)
        if p.exists() or p.is_symlink():
            found.append(_safe(root, p))
    if len(found) != 1:
        raise DailyRepairError("one existing daily month required; missing/twins refused")
    path = found[0]
    manifest_path = _safe(root, tdir / storage.MANIFEST_NAME)
    mb = manifest_path.read_bytes()
    manifest = _decode(mb)
    if storage.load_manifest(tdir) != manifest:
        raise DailyRepairError("manifest invalid or changed during read")
    conid = manifest.get("conid")
    if type(conid) is not int or conid <= 0:
        raise DailyRepairError("existing positive pinned conId required")
    raw = path.read_bytes()
    bars, stats = storage.read_month_file(path)
    key = storage.month_key(day.year, day.month)
    entry = manifest.get("intervals", {}).get("1d", {}).get("months", {}).get(key)
    if (not isinstance(entry, dict) or entry.get("sha256") != _sha(raw)
            or stats.get("sha256") != _sha(raw) or path.read_bytes() != raw
            or manifest_path.read_bytes() != mb):
        raise DailyRepairError("month/manifest identity mismatch")
    dates = _date_manifest(bars)
    old = [b for b in bars if b[0].date() == day]
    if len(old) != 1 or any(b[0].time() != time.min for b in bars):
        raise DailyRepairError("repair requires exactly one already-stored daily bar")
    return dict(ticker=ticker, day=day, path=path, tdir=tdir, stage=stage,
                manifest=manifest, manifest_bytes=mb, month_bytes=raw,
                bars=bars, old=old[0], dates=dates, key=key, conid=conid)


def plan(root, pairs, *, interval="1d"):
    """Read-only; no operation mint, recovery, adapter, port or ledger writes."""
    selected = explicit_pairs(pairs, interval)
    result = []
    for ticker, day in selected:
        # No ticker_transaction here: acquiring it can create a lock artifact.
        snap = _snapshot(root, ticker, day)
        result.append(dict(ticker=ticker, date=day.isoformat(), interval="1d",
                           stored=_encoded_bar(snap["old"]), conid=snap["conid"],
                           month_sha256=_sha(snap["month_bytes"]),
                           manifest_sha256=_sha(snap["manifest_bytes"])))
    return {"mode": "plan", "pairs": result, "sends": 0, "writes": 0}


def _fault(_phase):
    """Offline development seam; production callers cannot supply a callback."""


def _stage_files(stage, candidate):
    owned = {_MARKER, _PREPARING, candidate, storage.MANIFEST_NAME}
    entries = list(stage.iterdir())
    if any(p.name not in owned or not p.is_file() or p.is_symlink()
           or getattr(p.lstat(), "st_file_attributes", 0) & 1024
           for p in entries):
        raise DailyRepairError("foreign transaction-stage content; retained")
    return entries


def _clear_stage(root, stage, candidate):
    _safe(root, stage)
    entries = _stage_files(stage, candidate)
    # Keep ownership until the blocking stage is atomically detached. Deleting
    # the sentinel before rmdir would leave an unowned, unrecoverable empty stage
    # if the process died between those calls.
    for p in entries:
        if p.name != _PREPARING:
            p.unlink()
    _fault("cleanup_payloads_removed")
    finished = stage.parent / (".daily-repair-finished-" + uuid.uuid4().hex)
    _safe(root, finished, exists=False)
    os.rename(stage, finished)
    _fault("cleanup_stage_detached")
    (finished / _PREPARING).unlink()
    finished.rmdir()


def _payloads(wal):
    result = {}
    for name in ("month_before", "month_after", "manifest_before", "manifest_after"):
        try:
            raw = base64.b64decode(wal[name], validate=True)
        except (ValueError, TypeError, KeyError) as exc:
            raise DailyRepairError("invalid WAL payload") from exc
        if _sha(raw) != wal.get(name + "_sha256"):
            raise DailyRepairError("WAL payload hash mismatch")
        result[name] = raw
    return result


def _validate_recovery_transition(wal, payload, target, day):
    """A hash-consistent WAL still must describe precisely one daily repair."""
    parse = (storage._parse_parquet_payload if wal["format"] == "parquet"
             else storage._parse_month_payload)
    old = parse(payload["month_before"], target.name,
                window=storage.session_window("1d"))
    new = parse(payload["month_after"], target.name,
                window=storage.session_window("1d"))
    before_dates, after_dates = _date_manifest(old), _date_manifest(new)
    stamp = day.isoformat()
    if (set(before_dates) != set(after_dates) or stamp not in before_dates
            or before_dates[stamp] == after_dates[stamp]
            or any(before_dates[k] != after_dates[k] for k in before_dates if k != stamp)
            or any((b[0].year, b[0].month) != (day.year, day.month) for b in old + new)):
        raise DailyRepairError("WAL is not a single-date replacement")
    bm, am = _decode(payload["manifest_before"]), _decode(payload["manifest_after"])
    key = storage.month_key(day.year, day.month)
    try:
        be = bm["intervals"]["1d"]["months"][key]
        ae = am["intervals"]["1d"]["months"][key]
        history = am["daily_date_repairs"]
        previous = bm.get("daily_date_repairs", [])
        correction = history[-1]
        if (be["sha256"] != wal["month_before_sha256"]
                or ae["sha256"] != wal["month_after_sha256"]
                or history[:-1] != previous or len(history) != len(previous) + 1
                or correction["type"] != _KIND or correction["ticker"] != wal["ticker"]
                or correction["date"] != stamp
                or correction["old_day_sha256"] != before_dates[stamp]
                or correction["new_day_sha256"] != after_dates[stamp]
                or correction["old_month_sha256"] != wal["month_before_sha256"]
                or correction["new_month_sha256"] != wal["month_after_sha256"]
                or correction["dates_before"] != before_dates
                or correction["dates_after"] != after_dates
                or correction["old"] != _encoded_bar(next(b for b in old if b[0].date() == day))
                or correction["new"] != _encoded_bar(next(b for b in new if b[0].date() == day))):
            raise DailyRepairError("WAL correction record disagrees with payload")
        # Only the target cache entry, new history record and normal manifest
        # save metadata may differ. Other intervals/tickers cannot ride along.
        comparable = deepcopy(am)
        comparable["intervals"]["1d"]["months"][key] = deepcopy(be)
        for field in ("generation", "written_by", "daily_date_repairs"):
            if field in bm:
                comparable[field] = deepcopy(bm[field])
            else:
                comparable.pop(field, None)
        if comparable != bm or am["generation"] != bm.get("generation", 0) + 1:
            raise DailyRepairError("WAL changes unrelated manifest state")
    except (KeyError, TypeError, IndexError, StopIteration) as exc:
        raise DailyRepairError("WAL manifest/correction shape invalid") from exc


def _recover_locked(root, ticker):
    tdir = _safe(root, Path(root) / ticker)
    stage = tdir / storage.VOL_VALUE_RECONCILE_STAGE_DIR
    if not stage.exists():
        return {"status": "none"}
    _safe(root, stage)
    owner_path = _safe(root, stage / _PREPARING)
    owner = _decode(owner_path.read_bytes())
    if (not isinstance(owner, dict)
            or set(owner) not in ({"kind", "ticker", "candidate", "date"},
                {"kind", "ticker", "candidate", "date", "completed"})
            or owner["kind"] != _KIND or owner["ticker"] != ticker):
        raise DailyRepairError("stage belongs to another correction; retained")
    candidate = owner["candidate"]
    if not isinstance(candidate, str) or Path(candidate).name != candidate:
        raise DailyRepairError("unsafe staged candidate name")
    owner_day = explicit_pairs([{"ticker": ticker, "date": owner["date"]}])[0][1]
    fmt = Path(candidate).suffix[1:]
    if fmt not in {"csv", "parquet"}:
        raise DailyRepairError("unsafe staged candidate format")
    expected_target = storage.month_file_path(root, ticker, owner_day.year,
                                               owner_day.month, "1d", fmt=fmt)
    if candidate != expected_target.name:
        raise DailyRepairError("owner candidate/date mismatch")
    _stage_files(stage, candidate)
    marker = stage / _MARKER
    if not marker.exists():
        completed = owner.get("completed")
        if completed is not None:
            if (not isinstance(completed, dict)
                    or set(completed) != {"status", "month_sha256", "manifest_sha256"}
                    or completed["status"] not in {"committed", "rolled_back"}):
                raise DailyRepairError("invalid cleanup disposition")
            target = _safe(root, expected_target)
            manifest = _safe(root, tdir / storage.MANIFEST_NAME)
            if (_sha(target.read_bytes()) != completed["month_sha256"]
                    or _sha(manifest.read_bytes()) != completed["manifest_sha256"]):
                raise DailyRepairError("cleanup disposition no longer matches bank")
        _clear_stage(root, stage, candidate)
        return {"status": completed["status"] if completed else "aborted_preparation",
                "ticker": ticker, "date": owner["date"]}
    if marker.stat().st_size > _MAX_WAL_BYTES:
        raise DailyRepairError("oversized repair WAL")
    wal = _decode(marker.read_bytes())
    expected_keys = {"kind", "ticker", "date", "format"}
    for name in ("month_before", "month_after", "manifest_before", "manifest_after"):
        expected_keys.update({name, name + "_sha256"})
    if (not isinstance(wal, dict) or set(wal) != expected_keys
            or wal.get("kind") != _KIND or wal.get("ticker") != ticker):
        raise DailyRepairError("WAL identity mismatch")
    day = explicit_pairs([{"ticker": ticker, "date": wal.get("date")}])[0][1]
    if owner["date"] != day.isoformat():
        raise DailyRepairError("owner/WAL date mismatch")
    if wal.get("format") not in {"csv", "parquet"}:
        raise DailyRepairError("WAL format invalid")
    target = _safe(root, storage.month_file_path(
        root, ticker, day.year, day.month, "1d", fmt=wal["format"]))
    if target.name != candidate:
        raise DailyRepairError("WAL candidate identity mismatch")
    twin = storage.month_file_path(root, ticker, day.year, day.month, "1d",
                                   fmt="csv" if wal["format"] == "parquet" else "parquet")
    if twin.exists() or twin.is_symlink():
        raise DailyRepairError("ambiguous month twin during recovery; retained")
    payload = _payloads(wal)
    _validate_recovery_transition(wal, payload, target, day)
    mp = _safe(root, tdir / storage.MANIFEST_NAME)
    current_month, current_manifest = target.read_bytes(), mp.read_bytes()
    if current_manifest not in (payload["manifest_before"], payload["manifest_after"]):
        raise DailyRepairError("foreign manifest change during recovery; retained")
    if current_month == payload["month_before"]:
        # Month replacement is the commit point; an earlier crash rolls back.
        if current_manifest != payload["manifest_before"]:
            storage._atomic_write_bytes(mp, payload["manifest_before"])
        result = "rolled_back"
    elif current_month == payload["month_after"]:
        if current_manifest != payload["manifest_after"]:
            storage._atomic_write_bytes(mp, payload["manifest_after"])
        result = "committed"
    else:
        raise DailyRepairError("foreign month change during recovery; retained")
    expected = payload["manifest_after" if result == "committed" else "manifest_before"]
    if mp.read_bytes() != expected or target.read_bytes() != current_month:
        raise DailyRepairError("recovery readback failed; retained")
    _completed_owner(stage, owner, result, current_month, expected)
    _clear_stage(root, stage, candidate)
    return {"status": result, "ticker": ticker, "date": day.isoformat()}


def _completed_owner(stage, owner, status, month, manifest):
    storage._atomic_write_bytes(stage / _PREPARING, _json(dict(owner, completed={
        "status": status, "month_sha256": _sha(month), "manifest_sha256": _sha(manifest)})))


def _swap_locked(root, snap, replacement, operation_id):
    """Write only after the owned fetch path validated a detached daily bar."""
    fresh = _snapshot(root, snap["ticker"], snap["day"])
    if (fresh["month_bytes"] != snap["month_bytes"]
            or fresh["manifest_bytes"] != snap["manifest_bytes"]):
        raise DailyRepairError("stale pre-fetch snapshot; no replacement")
    if replacement[0] != snap["old"][0]:
        raise DailyRepairError("replacement date/time differs from stored daily label")
    record = {"type": _KIND, "ticker": snap["ticker"],
              "date": snap["day"].isoformat(), "operation_id": operation_id,
              "old": _encoded_bar(snap["old"]), "new": _encoded_bar(replacement),
              "old_day_sha256": snap["dates"][snap["day"].isoformat()],
              "new_day_sha256": _sha(_json(_encoded_bar(replacement))),
              "old_month_sha256": _sha(snap["month_bytes"])}
    if replacement == snap["old"]:
        return dict(record, status="verified_identical", writes=0,
                    new_month_sha256=_sha(snap["month_bytes"]),
                    published_manifest_sha256=_sha(snap["manifest_bytes"]),
                    dates_before=snap["dates"], dates_after=snap["dates"])
    bars = [replacement if b[0].date() == snap["day"] else b for b in snap["bars"]]
    after_dates = _date_manifest(bars)
    target_day = snap["day"].isoformat()
    if ({k: v for k, v in snap["dates"].items() if k != target_day}
            != {k: v for k, v in after_dates.items() if k != target_day}):
        raise DailyRepairError("candidate changed an unrelated daily date")
    stage, path = snap["stage"], snap["path"]
    owner = {"kind": _KIND, "ticker": snap["ticker"], "candidate": path.name,
             "date": target_day}
    # Publish the blocking stage only once its ownership record is durable.
    # A pre-publication crash may retain an inert preparation directory, never
    # an unowned blocking stage or any modification of stored bank values.
    preparation = Path(tempfile.mkdtemp(prefix=".daily-repair-preparing-", dir=snap["tdir"]))
    _safe(root, preparation)
    storage._atomic_write_bytes(preparation / _PREPARING, _json(owner))
    _fault("preparation_unpublished")
    if stage.exists() or stage.is_symlink():
        raise DailyRepairError("foreign correction stage appeared; preparation retained")
    os.rename(preparation, stage)
    _safe(root, stage)
    candidate = stage / path.name
    stats = storage.write_month_file(candidate, bars, verify_after_write=True)
    new_month = candidate.read_bytes()
    record["new_month_sha256"] = _sha(new_month)
    record["dates_before"], record["dates_after"] = snap["dates"], after_dates
    manifest = deepcopy(snap["manifest"])
    entry = manifest["intervals"]["1d"]["months"][snap["key"]]
    entry.update(stats)
    entry["status"] = "present"
    # Keep the durable correction history at ticker scope, not only in a
    # month cache entry that an ordinary future append can rebuild.
    history = manifest.setdefault("daily_date_repairs", [])
    if not isinstance(history, list):
        raise DailyRepairError("existing repair history malformed")
    history.append(record)
    storage.save_manifest(stage, manifest)
    new_manifest = (stage / storage.MANIFEST_NAME).read_bytes()
    # The candidate manifest is embedded in the WAL, not an extra recovery file.
    (stage / storage.MANIFEST_NAME).unlink()
    wal = dict(kind=_KIND, ticker=snap["ticker"], date=target_day,
               format=path.suffix[1:])
    for name, raw in (("month_before", snap["month_bytes"]), ("month_after", new_month),
                      ("manifest_before", snap["manifest_bytes"]),
                      ("manifest_after", new_manifest)):
        wal[name] = base64.b64encode(raw).decode("ascii")
        wal[name + "_sha256"] = _sha(raw)
    raw_wal = _json(wal)
    if len(raw_wal) > _MAX_WAL_BYTES:
        raise DailyRepairError("repair WAL exceeds bounded size")
    _validate_recovery_transition(wal, _payloads(wal), path, snap["day"])
    storage._atomic_write_bytes(stage / _MARKER, raw_wal)
    _fault("prepared")
    if (path.read_bytes() != snap["month_bytes"]
            or (snap["tdir"] / storage.MANIFEST_NAME).read_bytes() != snap["manifest_bytes"]):
        raise DailyRepairError("foreign change before publication; stage retained")
    storage._atomic_write_bytes(snap["tdir"] / storage.MANIFEST_NAME, new_manifest)
    _fault("manifest_published")
    os.replace(candidate, path)
    _fault("month_published")
    if (path.read_bytes() != new_month
            or (snap["tdir"] / storage.MANIFEST_NAME).read_bytes() != new_manifest):
        raise DailyRepairError("publication readback failed; stage retained")
    _completed_owner(stage, owner, "committed", new_month, new_manifest)
    _clear_stage(root, stage, path.name)
    return dict(record, status="replaced", writes=1,
                published_manifest_sha256=_sha(new_manifest))


def _bank_fingerprints(root):
    """Once per batch, compare all data files and ticker manifests, not sizes."""
    root = _safe(root, root)
    found = {}
    for path in root.rglob("*"):
        relative = path.relative_to(root)
        if ("_ingest_reports" in relative.parts
                or storage.VOL_VALUE_RECONCILE_STAGE_DIR in relative.parts):
            continue
        _safe(root, path)
        if path.is_file() and (storage.FILENAME_RE.fullmatch(path.name)
                               or path.name == storage.MANIFEST_NAME):
            found[relative.as_posix()] = _sha(path.read_bytes())
    return found


def _assert_untouched(before, after, allowed):
    if ({p: h for p, h in before.items() if p not in allowed}
            != {p: h for p, h in after.items() if p not in allowed}):
        raise DailyRepairError("unrequested ticker/interval/month changed")


@bridge.worker_scope
def _repair_one(root, snap, adapter, pacer, cancel=None):
    worker = bridge.current_worker()
    if worker is None:
        raise DailyRepairError("daily repair requires owned worker context")
    context, day = worker.context, snap["day"]
    window = context.authority.window("1d", day)
    horizon = context.horizons["1d"]
    if (window is None or horizon is None or window[1] > horizon
            or context.captured_now < context.authority.daily_settled_close(day)):
        raise DailyRepairError("requested daily session is not complete and settled")
    contract = adapter.contract_for(snap["conid"])
    contract.symbol = snap["ticker"]
    counters = Counter()
    response = ibkr._fetch_request(
        adapter, contract, window[1].replace(tzinfo=None), "1 D", "1 day",
        pacer, cancel, lambda message: None, lambda: None, snap["ticker"],
        {"requests": 0, "counters": counters}, day, metered=False,
        interval="1d", intended_start=datetime.combine(day, time.min),
        intended_end=window[1].replace(tzinfo=None))
    if len(response) != 1:
        raise DailyRepairError("exactly one complete requested daily bar required")
    bars = ibkr.convert_bars(response, day, counters, "1d")
    if len(bars) != 1 or bars[0][0].date() != day:
        raise DailyRepairError("invalid or wrong-date daily replacement")
    # Only detached, durable-result values reach the transaction. The stage
    # writer's strict codec check also refuses invalid/nonfinite OHLCV.
    with storage.ticker_transaction(snap["tdir"]):
        return _swap_locked(root, snap, bars[0], context.operation_id)


def run_daily_repair(root, pairs, *, interval="1d", ports=None,
                     adapter_factory=None, cancel=None, evidence_dir=None,
                     _test_capability=None):
    """Explicit A1 repair batch, not nightly update or a general rewrite API."""
    selected = explicit_pairs(pairs, interval)
    root = _safe(root, root)
    if adapter_factory is None:
        if (not isinstance(ports, (list, tuple)) or not ports
                or any(type(p) is not int or not 1 <= p <= 65535 for p in ports)):
            raise DailyRepairError("explicit valid ports required for execution")
        adapter_factory = ibkr.live_adapter_factory(
            host=ibkr.HOST_DEFAULT, ports=tuple(ports), client_id=ibkr.CLIENT_ID_FETCH)
    directory = (Path(evidence_dir) if evidence_dir is not None
                 else root / "_ingest_reports" / "fetch-ledgers")
    operation = fops.begin_operation(
        "daily_repair", directory, test_capability=_test_capability)
    context = operation.context
    rows, error, adapter = [], None, None
    before, after, allowed = {}, {}, set()
    try:
        fops.require_root_admission(operation)
        with operation_gate.acquire("fetch", owner="explicit daily date repair"):
            # Take snapshots before any connection. A pending transaction is
            # recovered only by the explicit recovery command, never plan().
            snaps = []
            for ticker, day in selected:
                with storage.ticker_transaction(_safe(root, root / ticker)):
                    snap = _snapshot(root, ticker, day)
                window = context.authority.window("1d", day)
                if (window is None or context.horizons["1d"] is None
                        or window[1] > context.horizons["1d"]
                        or context.captured_now < context.authority.daily_settled_close(day)):
                    raise DailyRepairError("unsettled/unsupported pair; no connection")
                snaps.append(snap)
            before = _bank_fingerprints(root)
            expected = dict(before)
            adapter = bridge.without_authority(adapter_factory)()
            for index, snap in enumerate(snaps):
                if cancel is not None and bridge.without_authority(cancel.is_set)():
                    raise ibkr.Cancelled()
                # Prior pairs in the same month legitimately updated its CAS;
                # re-snapshot just this explicit pair under the ticker lock.
                with storage.ticker_transaction(snap["tdir"]):
                    snap = _snapshot(root, snap["ticker"], snap["day"])
                month_key = snap["path"].relative_to(root).as_posix()
                manifest_key = (snap["tdir"] / storage.MANIFEST_NAME).relative_to(root).as_posix()
                if (expected.get(month_key) != _sha(snap["month_bytes"])
                        or expected.get(manifest_key) != _sha(snap["manifest_bytes"])):
                    raise DailyRepairError("foreign change since batch preflight")
                row = _repair_one(root, snap, adapter, ibkr.Pacer(), cancel,
                    _fetch_child=operation.child("daily-repair-" + str(index)))
                rows.append(row)
                expected[month_key] = row["new_month_sha256"]
                expected[manifest_key] = row["published_manifest_sha256"]
                if row["writes"]:
                    allowed.update({month_key, manifest_key})
            after = _bank_fingerprints(root)
            _assert_untouched(before, after, allowed)
            if after != expected:
                raise DailyRepairError("final bank differs from this batch's exact publications")
            operation.seal()
    except BaseException as exc:
        error = exc
    finally:
        if adapter is not None:
            try:
                bridge.without_authority(adapter.disconnect)()
            except BaseException as exc:
                error = error or exc
        try:
            operation.close()
        except BaseException as exc:
            error = error or exc
    report = reports.evidence(context, "daily_repair",
                              outcome="failed" if error else "returned", error=error)
    report["daily_repair"] = dict(pairs=[{"ticker": t, "date": d.isoformat()}
                                          for t, d in selected],
                                  rows=rows, before=before, after=after,
                                  allowed_changes=sorted(allowed))
    report["report_path"] = reports.write_report(report, directory)
    if error is not None:
        try:
            error.fetch_ledger = report
        except Exception:
            pass
        raise error
    return report


def recover(root, pairs, *, interval="1d"):
    """Explicit no-fetch recovery, bounded by the named ticker/date pairs."""
    selected = explicit_pairs(pairs, interval)
    root = _safe(root, root)
    result = []
    with operation_gate.acquire("fetch", owner="daily repair recovery"):
        for ticker, day in selected:
            tdir = _safe(root, root / ticker)
            with storage.ticker_transaction(tdir):
                owner_path = tdir / storage.VOL_VALUE_RECONCILE_STAGE_DIR / _PREPARING
                if owner_path.exists():
                    owner = _decode(_safe(root, owner_path).read_bytes())
                    if owner.get("date") != day.isoformat():
                        raise DailyRepairError("pending preparation date not explicitly selected")
                marker = tdir / storage.VOL_VALUE_RECONCILE_STAGE_DIR / _MARKER
                if marker.exists():
                    wal = _decode(_safe(root, marker).read_bytes())
                    if wal.get("date") != day.isoformat():
                        raise DailyRepairError("pending repair date not explicitly selected")
                result.append(_recover_locked(root, ticker))
    return {"mode": "recovery", "rows": result, "sends": 0}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bank", required=True)
    parser.add_argument("--pair", action="append", required=True, help="TICKER:YYYY-MM-DD")
    parser.add_argument("--interval", choices=["1d"], default="1d")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--execute", action="store_true")
    mode.add_argument("--recover", action="store_true")
    parser.add_argument("--port", type=int, action="append")
    args = parser.parse_args(argv)
    try:
        pairs = []
        for item in args.pair:
            ticker, day = item.split(":", 1)
            pairs.append({"ticker": ticker, "date": day})
        if args.execute:
            result = run_daily_repair(args.bank, pairs, interval=args.interval, ports=args.port)
        elif args.recover:
            result = recover(args.bank, pairs, interval=args.interval)
        else:
            result = plan(args.bank, pairs, interval=args.interval)
        print(json.dumps(result, sort_keys=True))
        return 0
    except (DailyRepairError, storage.StorageError, ValueError, OSError) as exc:
        print(json.dumps({"status": "REFUSED", "error": str(exc)}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
