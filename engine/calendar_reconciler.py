"""Calendar self-maintenance for bank-wide ad-hoc NYSE closures.

This is the production WS5a bridge between the data-derived bank calendar and
``market_calendar``. It is offline and writes only the bank-root
``_special_closures.json`` sidecar when a high-confidence market-wide closure is
missing from the static calendar.
"""
from __future__ import annotations

import datetime as dt
import json
import sys
from pathlib import Path

import market_calendar as mc
import stock_storage as ss
import stock_validate as sv


SPECIAL_CLOSURES_FILE = mc.SPECIAL_CLOSURES_SIDECAR
ACTIVE_WINDOW_DAYS = 4
VERSION = 1


def _iso_day(value):
    if isinstance(value, dt.datetime):
        return value.date().isoformat()
    if isinstance(value, dt.date):
        return value.isoformat()
    s = str(value or "").strip()
    if len(s) >= 10:
        try:
            return dt.date.fromisoformat(s[:10]).isoformat()
        except ValueError:
            return None
    return None


def _day_from_index(day_index):
    return dt.date.fromordinal(sv._EPOCH_ORD + int(day_index))


def _day_sets_from_cache(root):
    try:
        payload = json.loads((Path(root) / sv._CAL_FILE).read_text(
            encoding="utf-8"))
    except (OSError, ValueError, AttributeError):
        return {}
    tickers = payload.get("tickers") if isinstance(payload, dict) else None
    if not isinstance(tickers, dict):
        return {}
    out = {}
    for key, ent in tickers.items():
        days = ent.get("days") if isinstance(ent, dict) else None
        if not isinstance(days, list):
            continue
        parsed = set()
        for day_index in days:
            try:
                parsed.add(_day_from_index(day_index))
            except (TypeError, ValueError, OverflowError):
                pass
        if parsed:
            out[str(key)] = parsed
    return out


def _day_sets_direct(root, read_fn=None):
    out = {}
    for ticker, interval in sv.discover_series(root, rth_only=True):
        if ss.base_interval(str(interval)) != "1d":
            continue
        key = f"{ss.canonical_ticker(ticker)} {interval}"
        try:
            if read_fn is None:
                days = {
                    _day_from_index(sec // 86400)
                    for sec in sv.read_series_ts(root, ticker, interval)
                }
            else:
                days = sv._bar_days(read_fn(root, ticker, interval))
        except Exception:  # noqa: BLE001 - one bad series cannot block calendar
            continue
        if days:
            out[key] = set(days)
    return out


def series_day_sets(root, read_fn=None):
    """Return {"TICKER 1d": {date, ...}} for every daily bank series.

    The production path warms/reuses stock_validate's consensus calendar cache,
    then reads that sidecar instead of re-parsing every 1d file. Injected
    ``read_fn`` is for selftests and bypasses the cache.
    """
    root = Path(root)
    if read_fn is not None:
        return _day_sets_direct(root, read_fn=read_fn)
    try:
        sv.consensus_calendar(root, min_tickers=1)
        cached = _day_sets_from_cache(root)
        if cached:
            return cached
    except Exception:  # noqa: BLE001
        pass
    return _day_sets_direct(root)


def _active_around(day, consensus_days, window_days=ACTIVE_WINDOW_DAYS):
    one = dt.timedelta(days=1)
    for k in range(1, int(window_days) + 1):
        if (day - k * one) in consensus_days:
            return True
        if (day + k * one) in consensus_days:
            return True
    return False


def _closure_candidates(day_sets, min_tickers=2,
                        active_window_days=ACTIVE_WINDOW_DAYS):
    if not day_sets:
        return [], [], None
    series = []
    counts = {}
    for key, days in sorted(day_sets.items()):
        days = set(days or [])
        if not days:
            continue
        first, last = min(days), max(days)
        series.append((key, first, last, days))
        for day in days:
            counts[day] = counts.get(day, 0) + 1
    if not series:
        return [], [], None
    lo = min(first for _key, first, _last, _days in series)
    hi = max(last for _key, _first, last, _days in series)
    consensus = {day for day, n in counts.items() if n >= min_tickers}
    high_confidence = []
    ambiguous = []
    day = lo
    one = dt.timedelta(days=1)
    while day <= hi:
        if day.weekday() >= 5:
            day += one
            continue
        spanning = sum(1 for _key, first, last, _days in series
                       if first <= day <= last)
        traded = counts.get(day, 0)
        active = _active_around(day, consensus, active_window_days)
        if spanning >= min_tickers and traded == 0 and active:
            high_confidence.append({
                "date": day.isoformat(),
                "spanning_tickers": spanning,
                "traded_tickers": traded,
                "active_window_days": active_window_days,
            })
        elif spanning >= min_tickers and traded < min_tickers and active:
            ambiguous.append({
                "date": day.isoformat(),
                "spanning_tickers": spanning,
                "traded_tickers": traded,
                "why": "not market-wide zero-trade closure",
            })
        day += one
    return high_confidence, ambiguous, [lo.isoformat(), hi.isoformat()]


def load_sidecar(root):
    path = Path(root) / SPECIAL_CLOSURES_FILE
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {"kind": "special_closures", "version": VERSION,
                "closures": []}
    except (OSError, ValueError) as exc:
        return {"kind": "special_closures", "version": VERSION,
                "closures": [], "error": f"{type(exc).__name__}: {exc}"}
    if not isinstance(data, dict):
        return {"kind": "special_closures", "version": VERSION,
                "closures": [], "error": "sidecar is not a JSON object"}
    raw = data.get("closures", data.get("dates", []))
    closures = []
    if isinstance(raw, dict):
        raw = raw.values()
    if isinstance(raw, list):
        for item in raw:
            ent = dict(item) if isinstance(item, dict) else {"date": item}
            day = _iso_day(ent.get("date"))
            if not day:
                continue
            ent["date"] = day
            closures.append(ent)
    out = dict(data)
    out["kind"] = str(out.get("kind") or "special_closures")
    out["version"] = int(out.get("version") or VERSION)
    out["closures"] = sorted(closures, key=lambda e: e["date"])
    return out


def sidecar_dates(root):
    return sorted({c["date"] for c in load_sidecar(root).get("closures", [])})


def write_sidecar(root, sidecar):
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    clean = dict(sidecar or {})
    clean["kind"] = "special_closures"
    clean["version"] = VERSION
    closures = []
    seen = set()
    for ent in clean.get("closures") or []:
        if not isinstance(ent, dict):
            ent = {"date": ent}
        day = _iso_day(ent.get("date"))
        if not day or day in seen:
            continue
        seen.add(day)
        e2 = dict(ent)
        e2["date"] = day
        closures.append(e2)
    clean["closures"] = sorted(closures, key=lambda e: e["date"])
    payload = json.dumps(clean, indent=2, sort_keys=True).encode("utf-8")
    target = root / SPECIAL_CLOSURES_FILE
    ss._atomic_write_bytes(target, payload)
    mc.load_special_closures(root)
    return str(target)


def load_into_market_calendar(root):
    return mc.load_special_closures(root)


def reconcile(root, min_tickers=2, read_fn=None, is_trading_day_fn=None,
              asof=None, active_window_days=ACTIVE_WINDOW_DAYS):
    """Detect high-confidence data-derived closures missing from the calendar.

    This function is read-only. Use ``maintain(..., write=True)`` to auto-pin
    missing high-confidence closures to the sidecar.
    """
    root = Path(root)
    asof = asof or dt.datetime.now().isoformat(timespec="seconds")
    if is_trading_day_fn is None:
        mc.load_special_closures(root)
        is_trading_day_fn = mc.is_trading_day
    day_sets = series_day_sets(root, read_fn=read_fn)
    high_confidence, ambiguous, rng = _closure_candidates(
        day_sets, min_tickers=min_tickers,
        active_window_days=active_window_days)
    missing = [
        row["date"] for row in high_confidence
        if is_trading_day_fn(dt.date.fromisoformat(row["date"]))
    ]
    return {
        "kind": "calendar_reconciler",
        "version": VERSION,
        "asof": asof,
        "root": str(root),
        "min_tickers": min_tickers,
        "active_window_days": active_window_days,
        "range": rng,
        "series_count": len(day_sets),
        "data_closed": [row["date"] for row in high_confidence],
        "high_confidence": high_confidence,
        "ambiguous": ambiguous,
        "missing": missing,
        "sidecar_dates": sidecar_dates(root),
        "sidecar_path": str(root / SPECIAL_CLOSURES_FILE),
    }


def maintain(root, min_tickers=2, write=False, read_fn=None, asof=None,
             active_window_days=ACTIVE_WINDOW_DAYS):
    """Detect and optionally auto-pin missing high-confidence closures.

    With ``write=True`` this writes only ``_special_closures.json`` and reloads
    market_calendar's sidecar dates for this process. Corrupt sidecars are not
    overwritten; the report keeps the missing dates visible instead.
    """
    root = Path(root)
    asof = asof or dt.datetime.now().isoformat(timespec="seconds")
    before = reconcile(
        root, min_tickers=min_tickers, read_fn=read_fn, asof=asof,
        active_window_days=active_window_days)
    auto_added = []
    sidecar = load_sidecar(root)
    sidecar_error = sidecar.get("error")
    if write and before.get("missing") and not sidecar_error:
        existing = {c["date"] for c in sidecar.get("closures", [])}
        by_date = {r["date"]: r for r in before.get("high_confidence", [])}
        closures = list(sidecar.get("closures") or [])
        for day in before["missing"]:
            if day in existing:
                continue
            ev = by_date.get(day) or {}
            closures.append({
                "date": day,
                "source": "calendar_reconciler",
                "first_seen": asof,
                "last_seen": asof,
                "reason": "whole bank closed on a weekday while static calendar said trading",
                "evidence": {
                    "spanning_tickers": ev.get("spanning_tickers"),
                    "traded_tickers": ev.get("traded_tickers"),
                    "active_window_days": ev.get("active_window_days"),
                    "min_tickers": min_tickers,
                    "range": before.get("range"),
                },
            })
            existing.add(day)
            auto_added.append(day)
        if auto_added:
            sidecar["closures"] = closures
            sidecar["updated"] = asof
            write_sidecar(root, sidecar)
    after = reconcile(
        root, min_tickers=min_tickers, read_fn=read_fn, asof=asof,
        active_window_days=active_window_days)
    after["candidate_missing"] = before.get("missing", [])
    after["auto_added"] = auto_added
    if sidecar_error:
        after["sidecar_error"] = sidecar_error
    return after


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    write = "--write" in argv
    if "--no-write" in argv:
        write = False
    root = ss.storage_root(".")
    for arg in argv:
        if arg in ("--write", "--no-write"):
            continue
        if not arg.startswith("-"):
            root = Path(arg)
    report = maintain(root, write=write)
    print(f"calendar range: {report.get('range')}  "
          f"series={report.get('series_count')}")
    print("data-closed weekdays: " + str(len(report.get("data_closed") or [])))
    print("candidate missing: "
          + (", ".join(report.get("candidate_missing") or []) or "none"))
    print("auto-added: " + (", ".join(report.get("auto_added") or []) or "none"))
    print("still missing: " + (", ".join(report.get("missing") or []) or "none"))
    if report.get("sidecar_error"):
        print("sidecar error: " + str(report["sidecar_error"]))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
