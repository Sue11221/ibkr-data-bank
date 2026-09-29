"""Focused offline selftest for Row 51 M2 volatility-value safeguards."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from unittest import mock

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))

import export_quality as quality  # noqa: E402
import market_calendar  # noqa: E402
import stock_storage as storage  # noqa: E402
import vol_value_audit as audit_mod  # noqa: E402


FAILS = []
COUNT = 0


def check(name, condition, detail=""):
    global COUNT
    COUNT += 1
    ok = bool(condition)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}" + (
        "" if ok or not detail else f"  {detail}"))
    if not ok:
        FAILS.append(name)


def root_in(tmp, name):
    root = Path(tmp) / name / storage.STORAGE_DIR_NAME
    root.mkdir(parents=True)
    return root


def trading_days(start, count):
    out = []
    cursor = start
    while len(out) < count:
        if market_calendar.is_trading_day(
                cursor, special_closures=frozenset()):
            out.append(cursor)
        cursor += dt.timedelta(days=1)
    return out


def write_values(root, ticker, interval, values):
    grouped = {}
    for day, value in values:
        grouped.setdefault((day.year, day.month), []).append((day, value))
    paths = []
    for (year, month), pairs in sorted(grouped.items()):
        path = storage.month_file_path(
            root, ticker, year, month, interval)
        path.parent.mkdir(parents=True, exist_ok=True)
        bars = []
        for day, value in sorted(pairs):
            stamp = dt.datetime.combine(day, dt.time(15, 59))
            number = float(value)
            bars.append((stamp, number, number, number, number, 0))
        storage.write_month_file(path, bars)
        paths.append(path)
    return paths


def flagged_for(report, ticker, interval=None, day=None, reason=None):
    rows = []
    for row in report.get("flagged") or []:
        if row.get("ticker") != ticker:
            continue
        if interval is not None and row.get("kind_token") != interval:
            continue
        if day is not None and row.get("day") != day.isoformat():
            continue
        if reason is not None and reason not in (row.get("reasons") or []):
            continue
        rows.append(row)
    return rows


def month_hashes(root):
    return {
        str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(root.rglob("*.parquet"))
    }


def minimal_quality_report(settled):
    quality_counts = {name: 0 for name in quality.QUALITY_ASSESSMENTS}
    export_counts = {name: 0 for name in quality.EXPORT_STATUSES}
    export_counts.update({"MISSING_MONTHS": 0, "ROWS": 0})
    return {
        "kind": quality.REPORT_KIND,
        "version": quality.REPORT_VERSION,
        "batch": {
            "run_id": "vol-selftest",
            "generated_at": "2026-07-22T12:00:00+00:00",
            "state": "COMPLETE",
            "format": "csv",
            "interval": "1m-iv",
            "sessions": "rth",
            "start": "",
            "end": "",
            "destination": "fixture",
        },
        "summary": {
            "selected": 0,
            "present": 0,
            "export": export_counts,
            "quality": quality_counts,
            "source_confirmed_volatility_anomalies": len(settled),
            "source_confirmed_volatility_anomalies_total": len(settled),
            "source_confirmed_volatility_anomalies_truncated": False,
        },
        "rows": [],
        "source_confirmed_volatility_anomalies": settled,
        "sources": {"errors": []},
    }


def main():
    with tempfile.TemporaryDirectory(prefix="vol_value_audit_selftest_") as tmp:
        mon = dt.date(2024, 6, 17)
        tue = mon + dt.timedelta(days=1)
        wed = tue + dt.timedelta(days=1)
        thu = wed + dt.timedelta(days=1)  # Juneteenth is not a session.

        clean = root_in(tmp, "clean")
        clean_paths = write_values(
            clean, "CLEAN", "1m-iv", [(mon, 0.30), (tue, 0.31)])
        clean_paths += write_values(
            clean, "CLEAN", "1d-hvol", [(mon, 0.20), (tue, 0.21)])
        manifest_path = clean / "CLEAN" / storage.MANIFEST_NAME
        manifest_path.write_bytes(b'{"fixture":"unchanged"}')
        manifest_before = manifest_path.read_bytes()
        clean_before = month_hashes(clean)
        clean_report = audit_mod.audit(clean, write_queue=False)
        check("clean IV/HVOL series audit without findings",
              not clean_report["flagged"], repr(clean_report["flagged"]))
        check("write_queue=False writes no audit/queue/registry sidecar",
              all(not (clean / name).exists() for name in (
                  audit_mod.AUDIT_BASENAME, audit_mod.QUEUE_BASENAME,
                  audit_mod.SETTLED_BASENAME)))
        check("read-only audit preserves every market-data byte",
              month_hashes(clean) == clean_before,
              repr((clean_before, month_hashes(clean))))
        check("read-only audit preserves manifest bytes",
              manifest_path.read_bytes() == manifest_before)

        calendar_root = root_in(tmp, "calendar-root")
        cal_tue = dt.date(2024, 6, 11)
        cal_thu = dt.date(2024, 6, 13)
        write_values(calendar_root, "CAL", "1m-iv", [
            (cal_tue, 0.1), (cal_thu, 0.8),
        ])
        (calendar_root / market_calendar.SPECIAL_CLOSURES_SIDECAR).write_text(
            json.dumps({"closures": [{"date": "2024-06-12"}]}),
            encoding="utf-8")
        global_closures_before = market_calendar.loaded_special_closures()
        calendar_report = audit_mod.audit(calendar_root, write_queue=False)
        check("audit binds session adjacency to the supplied bank root",
              bool(flagged_for(
                  calendar_report, "CAL", "1m-iv", cal_thu, "jump_up"))
              and market_calendar.loaded_special_closures()
              == global_closures_before)
        audit_mod.audit(calendar_root, write_queue=True)
        (calendar_root / market_calendar.SPECIAL_CLOSURES_SIDECAR).write_text(
            "{malformed", encoding="utf-8")
        corrupt_calendar = audit_mod.audit(calendar_root, write_queue=True)
        corrupt_calendar_queue = json.loads(
            (calendar_root / audit_mod.QUEUE_BASENAME).read_text(
                encoding="utf-8"))
        check("existing malformed closure sidecar makes audit incomplete",
              corrupt_calendar["complete"] is False
              and any(error.get("source")
                      == market_calendar.SPECIAL_CLOSURES_SIDECAR
                      for error in corrupt_calendar["errors"])
              and any(row.get("ticker") == "CAL"
                      for row in corrupt_calendar_queue["rows"]))

        detectors = root_in(tmp, "detectors")
        write_values(detectors, "CEIL", "1m-iv", [(mon, 10.1)])
        write_values(detectors, "LIMIT", "1m-iv", [(mon, 10.0)])
        write_values(detectors, "JUP", "1m-iv", [(mon, 0.1), (tue, 0.8)])
        write_values(detectors, "JDN", "1m-iv", [(mon, 0.8), (tue, 0.1)])
        write_values(detectors, "ZERO", "1m-iv", [
            (mon, 0.0), (tue, 0.0), (thu, 0.0)])
        write_values(detectors, "ZTWO", "1m-iv", [
            (mon, 0.0), (tue, 0.0)])
        write_values(detectors, "BHIGH", "1m-iv", [(mon, 2.1)])
        write_values(detectors, "BHIGH", "1d-hvol", [(mon, 0.1)])
        write_values(detectors, "BLOW", "1m-iv", [(mon, 0.004)])
        write_values(detectors, "BLOW", "1d-hvol", [(mon, 0.1)])
        write_values(detectors, "BMIN", "1m-iv", [(mon, 0.005)])
        write_values(detectors, "BMIN", "1d-hvol", [(mon, 0.1)])
        write_values(detectors, "BMAX", "1m-iv", [(mon, 2.0)])
        write_values(detectors, "BMAX", "1d-hvol", [(mon, 0.1)])
        prior_days = trading_days(dt.date(2024, 1, 2), 20)
        write_values(detectors, "UFLIP", "1m-iv", [
            *[(day, 0.1) for day in prior_days],
            (dt.date(2024, 2, 5), 5.0),
        ])
        write_values(detectors, "U19", "1m-iv", [
            *[(day, 0.1) for day in prior_days[:19]],
            (dt.date(2024, 2, 5), 5.0),
        ])
        final_jan = storage.month_file_path(
            detectors, "FINAL", 2024, 1, "1m-iv")
        final_jan.parent.mkdir(parents=True)
        final_bars = []
        for day in prior_days:
            final_bars.extend([
                (dt.datetime.combine(day, dt.time(9, 30)),
                 4.0, 4.0, 4.0, 4.0, 0),
                (dt.datetime.combine(day, dt.time(15, 59)),
                 0.1, 0.1, 0.1, 0.1, 0),
            ])
        storage.write_month_file(final_jan, final_bars)
        write_values(detectors, "FINAL", "1m-iv", [
            (dt.date(2024, 2, 5), 5.0),
        ])
        detector_before = month_hashes(detectors)
        detector_report = audit_mod.audit(
            detectors, write_queue=True,
            now=dt.datetime(2026, 7, 22, 12, tzinfo=dt.timezone.utc))
        check("strictly-over ceiling is flagged",
              bool(flagged_for(detector_report, "CEIL", reason="hard_ceiling")))
        check("exact hard ceiling remains valid",
              not flagged_for(detector_report, "LIMIT", reason="hard_ceiling"))
        check("inclusive 8x upward session jump is flagged on the new day",
              bool(flagged_for(
                  detector_report, "JUP", "1m-iv", tue, "jump_up")))
        check("inclusive 1/8 downward session jump is flagged on the new day",
              bool(flagged_for(
                  detector_report, "JDN", "1m-iv", tue, "jump_down")))
        zero_days = flagged_for(
            detector_report, "ZERO", "1m-iv", reason="iv_zero_run")
        check("exact three-session IV zero run flags all three days",
              len(zero_days) == 3, repr(zero_days))
        check("two-session IV zero run remains below threshold",
              not flagged_for(
                  detector_report, "ZTWO", "1m-iv", reason="iv_zero_run"))
        check("upper coherence-band violation flags both exact series",
              len(flagged_for(
                  detector_report, "BHIGH", reason="iv_hvol_band")) == 2)
        check("lower coherence-band violation flags both exact series",
              len(flagged_for(
                  detector_report, "BLOW", reason="iv_hvol_band")) == 2)
        check("exact coherence-band endpoints remain valid",
              not flagged_for(
                  detector_report, "BMIN", reason="iv_hvol_band")
              and not flagged_for(
                  detector_report, "BMAX", reason="iv_hvol_band"))
        check("inclusive 50x trailing-month unit flip is flagged",
              bool(flagged_for(
                  detector_report, "UFLIP", "1m-iv",
                  dt.date(2024, 2, 5), "unit_flip_month")))
        check("unit flip requires at least twenty prior daily closes",
              not flagged_for(
                  detector_report, "U19", "1m-iv",
                  dt.date(2024, 2, 5), "unit_flip_month"))
        check("unit flip statistics use each session's final stored close",
              bool(flagged_for(
                  detector_report, "FINAL", "1m-iv",
                  dt.date(2024, 2, 5), "unit_flip_month")))
        queue_payload = json.loads(
            (detectors / audit_mod.QUEUE_BASENAME).read_text(encoding="utf-8"))
        audit_payload = json.loads(
            (detectors / audit_mod.AUDIT_BASENAME).read_text(encoding="utf-8"))
        check("audit and queue artifacts persist their versioned envelopes",
              queue_payload.get("kind") == "vol_value_queue"
              and queue_payload.get("version") == 1
              and audit_payload.get("kind") == "vol_value_audit"
              and audit_payload.get("version") == 1
              and audit_payload.get("queue_source", {}).get("sha256")
              == hashlib.sha256(
                  (detectors / audit_mod.QUEUE_BASENAME).read_bytes()
              ).hexdigest())
        check("artifact writes preserve all market-data bytes",
              month_hashes(detectors) == detector_before)

        rolling_root = root_in(tmp, "rolling-window")
        rolling_seed = trading_days(dt.date(2023, 1, 2), 20)
        rolling_values = [(day, 0.001) for day in rolling_seed]
        for offset in range(1, 13):
            month_index = offset
            year = 2023 + month_index // 12
            month_number = month_index % 12 + 1
            one_day = trading_days(
                dt.date(year, month_number, 1), 1)[0]
            rolling_values.append((one_day, 1.0))
        rolling_target = trading_days(dt.date(2024, 2, 1), 1)[0]
        rolling_values.append((rolling_target, 50.0))
        write_values(rolling_root, "ROLL", "1m-iv", rolling_values)
        rolling_report = audit_mod.audit(rolling_root, write_queue=False)
        check("unit-flip history evicts the thirteenth prior month",
              not flagged_for(
                  rolling_report, "ROLL", "1m-iv", rolling_target,
                  "unit_flip_month"))

        stale_history_root = root_in(tmp, "stale-history-window")
        stale_seed = trading_days(dt.date(2022, 1, 3), 20)
        stale_target = dt.date(2024, 2, 5)
        write_values(stale_history_root, "STALE", "1m-iv", [
            *[(day, 0.1) for day in stale_seed],
            (stale_target, 5.0),
        ])
        stale_history_report = audit_mod.audit(
            stale_history_root, write_queue=False)
        check("unit-flip baseline is limited to prior twelve calendar months",
              not flagged_for(
                  stale_history_report, "STALE", "1m-iv", stale_target,
                  "unit_flip_month"))

        settled_root = root_in(tmp, "settled")
        write_values(
            settled_root, "SET", "1m-iv", [(mon, 0.3), (tue, 2.4)])
        first = audit_mod.audit(settled_root, write_queue=True)
        check("settlement fixture begins flagged",
              bool(flagged_for(first, "SET", "1m-iv", tue, "jump_up")))
        audit_mod.record_settled(
            settled_root, "SET", "1m-iv", tue.isoformat(), 2.4,
            reason="jump", run="selftest", confirmed="2026-07-22")
        registry = json.loads(
            (settled_root / audit_mod.SETTLED_BASENAME).read_text(
                encoding="utf-8"))
        check("settled registry persists the exact required nested shape",
              registry == {"SET": {"1m-iv": {tue.isoformat(): {
                  "value": 2.4, "reason": "jump",
                  "confirmed": "2026-07-22", "run": "selftest",
              }}}}, repr(registry))
        suppressed = audit_mod.audit(settled_root, write_queue=True)
        check("same stored value suppresses its flagged day",
              not flagged_for(suppressed, "SET", "1m-iv", tue)
              and any(row.get("day") == tue.isoformat()
                      for row in suppressed.get("suppressed") or []))
        check("settlement suppression requires the exact normalized reason set",
              audit_mod._settlement_covers({"jump_up"}, "jump")
              and not audit_mod._settlement_covers(
                  {"jump_up", "iv_hvol_band"}, "jump"))
        write_values(settled_root, "SET", "1m-iv", [
            (mon, 0.3), (tue, 2.4 + audit_mod.SETTLE_TOL)])
        boundary = audit_mod.audit(settled_root, write_queue=True)
        check("inclusive SETTLE_TOL boundary remains suppressed",
              not flagged_for(boundary, "SET", "1m-iv", tue))
        changed_value = 2.4 + audit_mod.SETTLE_TOL + 0.000001
        write_values(settled_root, "SET", "1m-iv", [
            (mon, 0.3), (tue, changed_value)])
        changed = audit_mod.audit(settled_root, write_queue=True)
        changed_rows = flagged_for(changed, "SET", "1m-iv", tue)
        check("changed settled value re-flags with prior-value evidence",
              bool(changed_rows)
              and changed_rows[0].get("settled_value_changed") is True
              and changed_rows[0].get("settled_value") == 2.4,
              repr(changed_rows))
        export_rows = audit_mod.settled_for_export(
            settled_root, [("SET", "1m-iv")])
        check("historical settlement stays export-visible after re-flag",
              len(export_rows) == 1
              and export_rows[0]["day"] == tue.isoformat())
        check("settled export selection is exact by ticker and kind",
              not audit_mod.settled_for_export(
                  settled_root, [("SET", "1d-hvol")]))

        spike_root = root_in(tmp, "intraday-spike")
        spike_path = storage.month_file_path(
            spike_root, "SPIKE", mon.year, mon.month, "1m-iv")
        spike_path.parent.mkdir(parents=True)
        spike_meta = storage.write_month_file(spike_path, [
            (dt.datetime.combine(mon, dt.time(9, 30)),
             0.3, 11.0, 0.3, 0.3, 0),
            (dt.datetime.combine(mon, dt.time(15, 59)),
             0.3, 0.3, 0.3, 0.3, 0),
        ])
        audit_mod.audit(spike_root, write_queue=True)
        audit_mod.record_settled(
            spike_root, "SPIKE", "1m-iv", mon.isoformat(), 0.3,
            reason="hard_ceiling", run="selftest", confirmed="2026-07-22")
        same_spike = audit_mod.audit(spike_root, write_queue=True)
        storage.write_month_file(spike_path, [
            (dt.datetime.combine(mon, dt.time(9, 30)),
             0.3, 20.0, 0.3, 0.3, 0),
            (dt.datetime.combine(mon, dt.time(15, 59)),
             0.3, 0.3, 0.3, 0.3, 0),
        ], spike_meta)
        changed_spike = audit_mod.audit(spike_root, write_queue=True)
        check("close-only settlements never suppress intraday hard violations",
              bool(flagged_for(
                  same_spike, "SPIKE", "1m-iv", mon, "hard_ceiling"))
              and bool(flagged_for(
                  changed_spike, "SPIKE", "1m-iv", mon, "hard_ceiling")))

        normalized = [{
            "ticker": "SET", "export_status": "WRITTEN",
            "source_intervals": ["1m", "1m-iv"],
        }]
        joined = quality._load_settled_volatility(settled_root, normalized)
        check("export-quality seam uses actual written auxiliary intervals",
              len(joined["rows"]) == 1 and not joined["errors"])
        price_only = quality._load_settled_volatility(settled_root, [{
            "ticker": "SET", "export_status": "WRITTEN",
            "source_intervals": ["1m"],
        }])
        nonwritten = quality._load_settled_volatility(settled_root, [{
            "ticker": "SET", "export_status": "FAILED",
            "source_intervals": ["1m-iv"],
        }])
        check("price-only and non-written exports exclude settlement rows",
              not price_only["rows"] and not nonwritten["rows"])
        note = quality.render_text(minimal_quality_report(joined["rows"]))
        check("quality note renders the dedicated source-confirmed section",
              "SOURCE-CONFIRMED VOLATILITY ANOMALIES" in note
              and "SET 1m-iv 2024-06-18" in note)
        run_logs = Path(tmp) / "quality-run-logs"
        run_logs.mkdir()
        export_registry_before = (
            settled_root / audit_mod.SETTLED_BASENAME).read_bytes()
        built = quality.build_report(
            {
                "run_id": "vol-integrated", "format": "csv",
                "interval": "1m", "sessions": "rth",
                "start": "", "end": "", "destination": "fixture",
                "state": "COMPLETE",
            },
            ["SET"], ["SET"], {"SET": {
                "status": "WRITTEN", "rows": 2, "file": "SET.csv",
                "source_intervals": ["1m", "1m-iv"],
            }},
            storage_root=settled_root, run_logs_root=run_logs,
            tier0_adapter=object(),
            now=dt.datetime(2026, 7, 22, 12, tzinfo=dt.timezone.utc))
        check("build_report carries exact settled rows and source metadata",
              built["summary"]["source_confirmed_volatility_anomalies"] == 1
              and built["source_confirmed_volatility_anomalies"]
              == joined["rows"]
              and built["sources"]["vol_value_settled"]["entries"] == 1)
        check("build_report leaves the settlement registry byte-identical",
              (settled_root / audit_mod.SETTLED_BASENAME).read_bytes()
              == export_registry_before)

        long_root = root_in(tmp, "long-registry-text")
        long_text = "r" * audit_mod.MAX_TEXT
        audit_mod.record_settled(
            long_root, "LONG", "1m-iv", mon.isoformat(), 0.3,
            reason=long_text, run=long_text, confirmed="2026-07-22")
        long_loaded = quality._load_settled_volatility(long_root, [{
            "ticker": "LONG", "export_status": "WRITTEN",
            "source_intervals": ["1m-iv"],
        }])
        check("producer-maximum settlement text remains export-consumable",
              len(long_loaded["rows"]) == 1
              and long_loaded["rows"][0]["reason"] == long_text
              and not long_loaded["errors"])

        capped_root = root_in(tmp, "capped-registry")
        capped_days = {}
        capped_start = dt.date(2024, 1, 1)
        for index in range(quality.MAX_SETTLED_VOL_ROWS + 50):
            day = (capped_start + dt.timedelta(days=index)).isoformat()
            capped_days[day] = {
                "value": 0.3, "reason": "jump_up",
                "confirmed": "2026-07-22", "run": "cap-selftest",
            }
        (capped_root / audit_mod.SETTLED_BASENAME).write_text(
            json.dumps({"CAP": {"1m-iv": capped_days}},
                       sort_keys=True, separators=(",", ":")),
            encoding="utf-8")
        capped = quality._load_settled_volatility(capped_root, [{
            "ticker": "CAP", "export_status": "WRITTEN",
            "source_intervals": ["1m-iv"],
        }])
        capped_report = minimal_quality_report(capped["rows"])
        capped_report["summary"].update({
            "source_confirmed_volatility_anomalies_total": capped["total"],
            "source_confirmed_volatility_anomalies_truncated":
                capped["truncated"],
        })
        capped_note = quality.render_text(capped_report)
        check("large valid settlement registries cap and render truthfully",
              len(capped["rows"]) == quality.MAX_SETTLED_VOL_ROWS
              and capped["total"] == quality.MAX_SETTLED_VOL_ROWS + 50
              and capped["truncated"] is True
              and "Showing 500 of 550" in capped_note)

        partial = root_in(tmp, "partial")
        write_values(partial, "PA", "1m-iv", [(mon, 0.1), (tue, 0.8)])
        write_values(partial, "PB", "1m-iv", [(mon, 0.1), (tue, 0.8)])
        audit_mod.audit(partial, write_queue=True)
        write_values(partial, "PA", "1m-iv", [(mon, 0.1), (tue, 0.11)])
        audit_mod.audit(partial, tickers=["PA"], write_queue=True)
        partial_queue = json.loads(
            (partial / audit_mod.QUEUE_BASENAME).read_text(encoding="utf-8"))
        queue_tickers = {row["ticker"] for row in partial_queue["rows"]}
        check("partial audit replaces its slice and preserves unscanned rows",
              queue_tickers == {"PB"}, repr(partial_queue["rows"]))

        concurrent_root = root_in(tmp, "settle-during-scan")
        write_values(concurrent_root, "RACE", "1m-iv", [
            (mon, 0.1), (tue, 0.8),
        ])
        scan_ready = threading.Event()
        scan_release = threading.Event()
        concurrent_result = {}
        original_analyze = audit_mod._analyze_series

        def paused_analyze(*args, **kwargs):
            value = original_analyze(*args, **kwargs)
            scan_ready.set()
            if not scan_release.wait(5):
                raise AssertionError("settlement race release timed out")
            return value

        def run_concurrent_audit():
            try:
                concurrent_result["report"] = audit_mod.audit(
                    concurrent_root, write_queue=True)
            except Exception as exc:  # noqa: BLE001
                concurrent_result["error"] = exc

        with mock.patch.object(
                audit_mod, "_analyze_series", side_effect=paused_analyze):
            audit_thread = threading.Thread(target=run_concurrent_audit)
            audit_thread.start()
            if not scan_ready.wait(5):
                concurrent_result["error"] = AssertionError(
                    "audit did not reach settlement race boundary")
            else:
                audit_mod.record_settled(
                    concurrent_root, "RACE", "1m-iv", tue.isoformat(), 0.8,
                    reason="jump", run="race-selftest",
                    confirmed="2026-07-22")
            scan_release.set()
            audit_thread.join(10)
        concurrent_queue = json.loads(
            (concurrent_root / audit_mod.QUEUE_BASENAME).read_text(
                encoding="utf-8"))
        check("audit reloads settlements before durable queue publication",
              not audit_thread.is_alive()
              and "error" not in concurrent_result
              and not concurrent_result["report"]["flagged"]
              and not concurrent_queue["rows"],
              repr(concurrent_result))

        mixed_root = root_in(tmp, "readable-plus-corrupt")
        jan_mon = dt.date(2024, 1, 8)
        jan_tue = jan_mon + dt.timedelta(days=1)
        write_values(mixed_root, "MIX", "1m-iv", [
            (jan_mon, 0.1), (jan_tue, 0.8),
        ])
        bad_month = storage.month_file_path(
            mixed_root, "MIX", 2024, 2, "1m-iv")
        bad_month.parent.mkdir(parents=True, exist_ok=True)
        bad_month.write_bytes(b"not-a-parquet-month")
        mixed_report = audit_mod.audit(mixed_root, write_queue=True)
        mixed_queue = json.loads(
            (mixed_root / audit_mod.QUEUE_BASENAME).read_text(
                encoding="utf-8"))
        check("readable findings survive another corrupt month for the ticker",
              bool(flagged_for(
                  mixed_report, "MIX", "1m-iv", jan_tue, "jump_up"))
              and any(row.get("ticker") == "MIX"
                      and row.get("day") == jan_tue.isoformat()
                      for row in mixed_queue["rows"]))

        empty_root = root_in(tmp, "empty-canonical-month")
        empty_path = write_values(empty_root, "EMPTY", "1m-iv", [
            (jan_mon, 0.1), (jan_tue, 0.8),
        ])[0]
        audit_mod.audit(empty_root, write_queue=True)
        empty_path.write_bytes(storage._bars_to_parquet([]))
        empty_report = audit_mod.audit(empty_root, write_queue=True)
        empty_queue = json.loads(
            (empty_root / audit_mod.QUEUE_BASENAME).read_text(
                encoding="utf-8"))
        check("empty canonical month is incomplete and preserves its queue slice",
              empty_report["complete"] is False
              and any("empty" in error.get("error", "")
                      for error in empty_report["errors"])
              and any(row.get("ticker") == "EMPTY"
                      and row.get("day") == jan_tue.isoformat()
                      for row in empty_queue["rows"]))

        same_key_root = root_in(tmp, "same-key-corruption")
        same_key_path = storage.month_file_path(
            same_key_root, "SAME", 2024, 1, "1m-iv")
        same_key_path.parent.mkdir(parents=True, exist_ok=True)
        storage.write_month_file(same_key_path, [
            (dt.datetime.combine(jan_mon, dt.time(15, 59)),
             0.1, 0.1, 0.1, 0.1, 0),
            (dt.datetime.combine(jan_tue, dt.time(9, 30)),
             0.8, 20.0, 0.8, 0.8, 0),
            (dt.datetime.combine(jan_tue, dt.time(15, 59)),
             0.8, 0.8, 0.8, 0.8, 0),
        ])
        audit_mod.audit(same_key_root, write_queue=True)
        same_key_path.write_bytes(storage._bars_to_parquet([
            (dt.datetime.combine(jan_mon, dt.time(15, 59)),
             0.1, 0.1, 0.1, 0.1, 0),
            (dt.datetime.combine(jan_tue, dt.time(15, 59)),
             0.8, 0.8, 0.8, 0.8, 0),
            (dt.datetime.combine(jan_tue, dt.time(9, 30)),
             0.8, 20.0, 0.8, 0.8, 0),
        ]))
        same_key_report = audit_mod.audit(same_key_root, write_queue=True)
        same_key_queue = json.loads(
            (same_key_root / audit_mod.QUEUE_BASENAME).read_text(
                encoding="utf-8"))
        same_key_row = next(
            row for row in same_key_queue["rows"]
            if row["ticker"] == "SAME" and row["day"] == jan_tue.isoformat())
        check("incomplete same-key scan unions rather than narrows old reasons",
              same_key_report["complete"] is False
              and same_key_row["reasons"] == ["hard_ceiling", "jump_up"],
              repr(same_key_row))

        corrupt = root_in(tmp, "corrupt")
        write_values(corrupt, "BAD", "1m-iv", [(mon, 0.1), (tue, 0.8)])
        bad_path = corrupt / audit_mod.SETTLED_BASENAME
        bad_path.write_text("[]", encoding="utf-8")
        bad_before = bad_path.read_bytes()
        corrupt_report = audit_mod.audit(corrupt, write_queue=False)
        check("corrupt registry cannot suppress and is surfaced as an error",
              bool(flagged_for(corrupt_report, "BAD", "1m-iv", tue))
              and bool(corrupt_report["errors"]))
        try:
            audit_mod.record_settled(
                corrupt, "BAD", "1m-iv", tue.isoformat(), 0.8,
                reason="jump", run="selftest")
            refused = False
        except audit_mod.VolValueAuditError:
            refused = True
        check("record_settled refuses and preserves a corrupt registry",
              refused and bad_path.read_bytes() == bad_before)
        malformed_quality = quality._load_settled_volatility(corrupt, [{
            "ticker": "BAD", "export_status": "WRITTEN",
            "source_intervals": ["1m-iv"],
        }])
        check("malformed settlement evidence becomes a bounded report warning",
              not malformed_quality["rows"]
              and malformed_quality["errors"][0]["source"]
              == "vol_value_settled")

        huge_root = root_in(tmp, "huge-number")
        write_values(huge_root, "HUGE", "1m-iv", [
            (mon, 0.1), (tue, 0.8),
        ])
        huge_raw = (
            '{"HUGE":{"1m-iv":{"2024-06-18":{"confirmed":'
            '"2026-07-22","reason":"jump_up","run":"selftest",'
            '"value":' + ("1" + "0" * 400) + '}}}}')
        (huge_root / audit_mod.SETTLED_BASENAME).write_text(
            huge_raw, encoding="utf-8")
        huge_report = audit_mod.audit(huge_root, write_queue=False)
        try:
            audit_mod.settled_for_export(
                huge_root, [("HUGE", "1m-iv")])
            huge_refused = False
        except audit_mod.VolValueAuditError:
            huge_refused = True
        check("overflowing registry numbers fail closed without raw exceptions",
              bool(flagged_for(huge_report, "HUGE", "1m-iv", tue))
              and bool(huge_report["errors"])
              and huge_refused)

        duplicate_root = root_in(tmp, "duplicate-key")
        duplicate_entry = (
            '{"value":0.8,"reason":"jump_up",'
            '"confirmed":"2026-07-22","run":"selftest"}')
        duplicate_raw = (
            '{"DUP":{"1m-iv":{"2024-06-18":' + duplicate_entry
            + ',"2024-06-18":' + duplicate_entry + '}}}')
        duplicate_path = duplicate_root / audit_mod.SETTLED_BASENAME
        duplicate_path.write_text(duplicate_raw, encoding="utf-8")
        duplicate_before = duplicate_path.read_bytes()
        try:
            audit_mod.settled_for_export(
                duplicate_root, [("DUP", "1m-iv")])
            duplicate_refused = False
        except audit_mod.VolValueAuditError:
            duplicate_refused = True
        check("duplicate registry keys fail closed and preserve source bytes",
              duplicate_refused
              and duplicate_path.read_bytes() == duplicate_before)

        invalid_tickers_refused = True
        for invalid in (None, True):
            try:
                audit_mod.settled_for_export(
                    duplicate_root, [(invalid, "1m-iv")])
            except audit_mod.VolValueAuditError:
                continue
            invalid_tickers_refused = False
        check("ticker inputs require real nonempty strings",
              invalid_tickers_refused)

        process_root = root_in(tmp, "process-settlements")
        engine_dir = str(Path(__file__).resolve().parent)
        child_code = (
            "import sys; sys.path.insert(0, sys.argv[1]); "
            "import vol_value_audit as a; "
            "a.record_settled(sys.argv[2], sys.argv[3], '1m-iv', "
            "'2024-06-18', 0.8, reason='jump_up', run='child', "
            "confirmed='2026-07-22')")
        children = [
            subprocess.Popen(
                [sys.executable, "-c", child_code, engine_dir,
                 str(process_root), f"P{index}"],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            for index in range(8)
        ]
        child_results = [child.communicate(timeout=30) for child in children]
        process_registry = json.loads(
            (process_root / audit_mod.SETTLED_BASENAME).read_text(
                encoding="utf-8"))
        check("cross-process settlement updates merge without lost entries",
              all(child.returncode == 0 for child in children)
              and set(process_registry) == {f"P{index}" for index in range(8)},
              repr(child_results))

        synthetic_rows = 500_000
        synthetic_start = int((dt.datetime(2024, 1, 1)
                               - dt.datetime(1970, 1, 1)).total_seconds())
        synthetic_ts = synthetic_start + np.arange(
            synthetic_rows, dtype=np.int64) * 5
        synthetic_values = np.full(synthetic_rows, 0.3, dtype=np.float64)
        synthetic_volume = np.zeros(synthetic_rows, dtype=np.int64)
        synthetic_issues = {}
        synthetic_errors = []
        synthetic_failed = set()
        started = time.perf_counter()
        with mock.patch.object(
                storage, "read_month_file_cols",
                return_value=(
                    synthetic_ts, synthetic_values, synthetic_values,
                    synthetic_values, synthetic_values, synthetic_volume)):
            _daily, _months, synthetic_seen, _complete = (
                audit_mod._analyze_series(
                    "PERF", "1m-iv", [{
                        "month": "2024-01", "path": Path("fixture.parquet"),
                        "manifest_sha": "a" * 64,
                    }], synthetic_issues, synthetic_errors,
                    synthetic_failed, frozenset()))
        elapsed = time.perf_counter() - started
        print(
            f"[INFO] vectorized detector benchmark: {synthetic_rows:,} rows "
            f"in {elapsed:.3f}s "
            f"({synthetic_rows / max(elapsed, 1e-9):,.0f} rows/s)")
        check("vectorized month analysis sustains production-scale detector CPU",
              synthetic_seen == synthetic_rows and elapsed < 5.0,
              f"{synthetic_seen} rows in {elapsed:.3f}s")

    print(f"\n{COUNT} checks, {len(FAILS)} failed")
    if FAILS:
        for name in FAILS:
            print(f"  - {name}")
        return 1
    print("VOL VALUE AUDIT SELFTEST PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
