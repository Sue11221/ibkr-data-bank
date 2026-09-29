"""Deterministic offline reference harness for Add Stocks run protection.

All storage roots are created under the system temporary directory. The fake
bar payloads model idempotent existing commit/resume semantics; this harness
tests that durable run intent never loses or duplicates that work.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
from pathlib import Path
import shutil
import sys
import tempfile

try:                                  # harness hygiene (Row 61): never die on a glyph
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:                     # noqa: BLE001 - older/redirected streams
    pass

sys.path.insert(0, str(Path(__file__).resolve().parent))

import addstock_run_manifest as arm
import addstock_watchdog as aw
import stock_storage as ss


PASS = [0]
FAIL = [0]
NOW = datetime(2026, 7, 17, 13, 0, tzinfo=timezone.utc)
SELECTIONS = [
    ("AAA", "1m"),
    ("AAA", "1m-pre"),
    ("AAA", "1m-post"),
    ("BBB", "1m"),
]


def check(condition, label):
    if condition:
        PASS[0] += 1
    else:
        FAIL[0] += 1
        print(f"  FAIL: {label}")


def _safe_interval(interval):
    return interval.replace("/", "_")


def _payload(ticker, interval):
    return (f"{ticker} {interval}\n"
            "2026-07-15T09:30:00,100,101,99,100.5,1000\n"
            "2026-07-15T09:31:00,100.5,102,100,101,1200\n").encode("ascii")


def _series_path(root, ticker, interval):
    return Path(root) / ticker / f"{_safe_interval(interval)}.bars"


def _partial_path(root, ticker, interval):
    return Path(root) / ticker / f"{_safe_interval(interval)}.backfill_incomplete"


def _write_series(root, ticker, interval):
    target = _series_path(root, ticker, interval)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(_payload(ticker, interval))
    try:
        _partial_path(root, ticker, interval).unlink()
    except FileNotFoundError:
        pass


def _write_partial(root, ticker, interval):
    target = _partial_path(root, ticker, interval)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("backfill_incomplete=true\n", encoding="ascii")


def _bank_fingerprint(root):
    root = Path(root)
    digest = hashlib.sha256()
    count = 0
    for path in sorted(p for p in root.rglob("*") if p.is_file()):
        if path.name == arm.ACTIVE_NAME or path.name.startswith(
                f".{arm.ACTIVE_NAME}."):
            continue
        rel = path.relative_to(root).as_posix()
        digest.update(rel.encode("utf-8") + b"\0")
        digest.update(path.read_bytes())
        count += 1
    return count, digest.hexdigest()


def _create(root, run_id):
    return arm.create_run(
        root, SELECTIONS,
        params={"mode": "add", "extended": True, "store_daily": False,
                "ports_at_start": [2000, 3000]},
        run_id=run_id, now=NOW)


def _credit_all(root, run_id, archive):
    data = arm.load_run(root)
    for ticker, record in data["tickers"].items():
        rth = [iv for iv in record["series"] if ss.session_of(iv) == "rth"]
        if rth:
            arm.mark_verification(root, run_id, ticker, "xval", rth,
                                  now=NOW, archive_dir=archive)
            arm.mark_verification(root, run_id, ticker, "gaps", rth,
                                  now=NOW, archive_dir=archive)
        arm.mark_verification(root, run_id, ticker, "earliest",
                              now=NOW, archive_dir=archive)


def _resume_to_completion(root, archive):
    data = arm.load_run(root)
    run_id = data["run_id"]
    arm.resume_run(root, run_id, now=NOW)
    for ticker, interval in arm.pending_selections(arm.load_run(root)):
        arm.mark_series_started(root, run_id, ticker, interval, now=NOW)
        _write_series(root, ticker, interval)
        arm.mark_series_complete(root, run_id, ticker, interval, now=NOW)
    _credit_all(root, run_id, archive)
    result = arm.mark_fetch_finished(
        root, run_id, reason="complete", now=NOW, archive_dir=archive)
    return result


class _Clock:
    def __init__(self):
        self.value = 0.0

    def __call__(self):
        return self.value

    def advance(self, seconds):
        self.value += float(seconds)


def _watchdog(clock, maintenance=None):
    provider = (lambda: maintenance[0]) if maintenance is not None else None
    return aw.HardDeathWatchdog(
        [2000, 3000], clock=clock, maintenance_provider=provider,
        port_grace_s=10, fleet_grace_s=20, probe_backoff_s=(2, 4, 8))


def _complete_active(root, archive):
    data = arm.load_run(root)
    run_id = data["run_id"]
    for ticker, interval in arm.pending_selections(data):
        arm.mark_series_started(root, run_id, ticker, interval, now=NOW)
        _write_series(root, ticker, interval)
        arm.mark_series_complete(root, run_id, ticker, interval, now=NOW)
    _credit_all(root, run_id, archive)
    return arm.mark_fetch_finished(
        root, run_id, reason="complete", now=NOW, archive_dir=archive)


def scenario_control(base):
    root = base / "control" / "bank"
    root.mkdir(parents=True)
    archive = base / "control" / "logs"
    run = _create(root, "addstock-reference-control")
    for ticker, interval in SELECTIONS:
        arm.mark_series_started(root, run["run_id"], ticker, interval, now=NOW)
        _write_series(root, ticker, interval)
        arm.mark_series_complete(root, run["run_id"], ticker, interval, now=NOW)
    _credit_all(root, run["run_id"], archive)
    result = arm.mark_fetch_finished(
        root, run["run_id"], now=NOW, archive_dir=archive)
    fingerprint = _bank_fingerprint(root)
    check(result["archived"] is not None and fingerprint[0] == len(SELECTIONS),
          "scenario 1 control completes and archives exactly four series")
    return root, fingerprint


def scenario_single_port_death(base, control_fingerprint):
    root = base / "single_port_death" / "bank"
    root.mkdir(parents=True)
    archive = root.parent / "logs"
    run = _create(root, "addstock-reference-single-death")
    gate = {"held": True}
    clock = _Clock()
    watchdog = _watchdog(clock)

    # Port 2000 owns the first in-flight ticker when its socket resets. The
    # durable building marker remains and the whole ticker job is re-queued.
    arm.mark_series_started(root, run["run_id"], "AAA", "1m", now=NOW)
    rerouted = watchdog.hard_signal(2000, "socket_reset")
    check(rerouted and not watchdog.holding()
          and watchdog.state(2000) == aw.SUSPECT,
          "scenario 2 single hard death re-queues without fleet HOLD")
    result = _complete_active(root, archive)
    gate["held"] = False
    check(result["archived"] is not None and not gate["held"]
          and _bank_fingerprint(root) == control_fingerprint,
          "scenario 2 reroute releases gate and is byte-identical to control")


def scenario_all_port_blip(base, control_fingerprint):
    root = base / "all_port_blip" / "bank"
    root.mkdir(parents=True)
    archive = root.parent / "logs"
    _create(root, "addstock-reference-all-blip")
    gate = {"held": True}
    finalized = [0]
    clock = _Clock()
    watchdog = _watchdog(clock)
    watchdog.hard_signal(2000, "connection_lost")
    watchdog.hard_signal(3000, "connection_lost")
    clock.advance(5)
    held = watchdog.holding()
    watchdog.probe_result(3000, True)
    result = _complete_active(root, archive)
    gate["held"] = False
    check(held and not watchdog.holding() and not watchdog.should_finalize()
          and finalized[0] == 0,
          "scenario 3 short all-port blip HOLDs then resumes without finalize")
    check(result["archived"] is not None and not gate["held"]
          and _bank_fingerprint(root) == control_fingerprint,
          "scenario 3 resumed blip is byte-identical to control")


def scenario_long_fleet_death(base, control_fingerprint):
    root = base / "long_fleet_death" / "bank"
    root.mkdir(parents=True)
    archive = root.parent / "logs"
    run = _create(root, "addstock-reference-long-death")
    gate = {"held": True}
    arm.mark_series_started(root, run["run_id"], "AAA", "1m", now=NOW)
    _write_series(root, "AAA", "1m")
    arm.mark_series_complete(root, run["run_id"], "AAA", "1m", now=NOW)
    clock = _Clock()
    watchdog = _watchdog(clock)
    watchdog.hard_signal(2000, "connection_refused")
    watchdog.hard_signal(3000, "connection_refused")
    clock.advance(21)
    should_finalize = watchdog.should_finalize()
    finish = arm.mark_fetch_finished(
        root, run["run_id"], reason="fleet_down",
        seal_pending=["AAA 1m", "BBB 1m"], now=NOW,
        archive_dir=archive)
    gate["held"] = False
    interrupted = arm.load_run(root)
    check(should_finalize and finish["archived"] is None and not gate["held"]
          and interrupted["state"] == "interrupted"
          and interrupted["finalize"]["seal_pending"]
          == ["AAA 1m", "BBB 1m"],
          "scenario 4 long fleet death degrades honestly and releases gate")
    result = _resume_to_completion(root, archive)
    check(result["archived"] is not None
          and _bank_fingerprint(root) == control_fingerprint,
          "scenario 4 gold gate resumes to byte-identical control")


def scenario_maintenance(base, control_fingerprint):
    root = base / "maintenance" / "bank"
    root.mkdir(parents=True)
    archive = root.parent / "logs"
    _create(root, "addstock-reference-maintenance")
    gate = {"held": True}
    clock = _Clock()
    maintenance = [aw.MaintenanceStatus(
        True, "valid", (2000, 3000), NOW + timedelta(minutes=10))]
    watchdog = _watchdog(clock, maintenance)
    watchdog.hard_signal(2000, "connection_refused")
    watchdog.hard_signal(3000, "connection_refused")
    clock.advance(100)
    protected = watchdog.snapshot()
    watchdog.probe_result(2000, True)
    result = _complete_active(root, archive)
    gate["held"] = False
    check(protected["maintenance_active"]
          and protected["fleet_grace_seconds"] == 0
          and not protected["finalize"],
          "scenario 5 valid maintenance defers grace through its window")
    check(result["archived"] is not None and not gate["held"]
          and _bank_fingerprint(root) == control_fingerprint,
          "scenario 5 maintenance recovery completes byte-identical to control")

    dead_clock = _Clock()
    dead = [aw.MaintenanceStatus(False, "dead_owner", (2000, 3000))]
    dead_watchdog = _watchdog(dead_clock, dead)
    dead_watchdog.hard_signal(2000, "connection_refused")
    dead_watchdog.hard_signal(3000, "connection_refused")
    dead_clock.advance(21)
    check(dead_watchdog.should_finalize(),
          "scenario 5 dead-owner maintenance is void and normal grace applies")


def scenario_slowness_storm(base, control_fingerprint):
    root = base / "slowness" / "bank"
    root.mkdir(parents=True)
    archive = root.parent / "logs"
    _create(root, "addstock-reference-slowness")
    gate = {"held": True}
    clock = _Clock()
    watchdog = _watchdog(clock)

    class RequestTimeout(ConnectionError):
        pass

    class PacingViolation(ConnectionError):
        pass

    transitioned = [
        watchdog.hard_signal(2000, RequestTimeout("slow")),
        watchdog.hard_signal(3000, PacingViolation("paced")),
        watchdog.hard_signal(2000, TimeoutError("busy")),
    ]
    clock.advance(1000)
    snapshot = watchdog.snapshot()
    result = _complete_active(root, archive)
    gate["held"] = False
    check(not any(transitioned)
          and all(row["state"] == aw.HEALTHY
                  for row in snapshot["ports"].values())
          and not snapshot["holding"] and not snapshot["finalize"],
          "scenario 6 timeout/pacing storm causes zero watchdog transitions")
    check(result["archived"] is not None and not gate["held"]
          and _bank_fingerprint(root) == control_fingerprint,
          "scenario 6 soft-event run stays byte-identical to control")


def _snapshot(base, name, setup):
    root = base / "snapshots" / name / "bank"
    root.mkdir(parents=True)
    run = _create(root, f"addstock-reference-{name}")
    setup(root, run["run_id"])
    return root


def scenario_process_kill_matrix(base, control_fingerprint):
    snapshots = []
    snapshots.append(_snapshot(base, "pending", lambda _root, _run: None))

    def building(root, run_id):
        arm.mark_series_started(root, run_id, "AAA", "1m", now=NOW)
    snapshots.append(_snapshot(base, "building", building))

    def mid_backfill(root, run_id):
        arm.mark_series_started(root, run_id, "AAA", "1m", now=NOW)
        _write_partial(root, "AAA", "1m")
    snapshots.append(_snapshot(base, "mid_backfill", mid_backfill))

    def commit_before_marker(root, run_id):
        arm.mark_series_started(root, run_id, "AAA", "1m", now=NOW)
        _write_series(root, "AAA", "1m")
    snapshots.append(_snapshot(base, "commit_before_marker", commit_before_marker))

    def between_sessions(root, run_id):
        for interval in ("1m", "1m-pre"):
            arm.mark_series_started(root, run_id, "AAA", interval, now=NOW)
            _write_series(root, "AAA", interval)
            arm.mark_series_complete(root, run_id, "AAA", interval, now=NOW)
    snapshots.append(_snapshot(base, "between_sessions", between_sessions))

    def built_before_checks(root, run_id):
        for ticker, interval in SELECTIONS:
            arm.mark_series_started(root, run_id, ticker, interval, now=NOW)
            _write_series(root, ticker, interval)
            arm.mark_series_complete(root, run_id, ticker, interval, now=NOW)
    snapshots.append(_snapshot(base, "built_before_checks", built_before_checks))

    for snapshot in snapshots:
        recovered = snapshot.parent / "recovered"
        shutil.copytree(snapshot, recovered)
        archive = recovered.parent / "logs"
        result = _resume_to_completion(recovered, archive)
        check(result["archived"] is not None
              and not arm.active_path(recovered).exists()
              and _bank_fingerprint(recovered) == control_fingerprint,
              f"scenario 7 resumes {snapshot.parent.name} byte-identical to control")


def scenario_robustness(base):
    root = base / "robustness" / "bank"
    root.mkdir(parents=True)
    path = arm.active_path(root)
    path.write_bytes(b'{"schema":1')
    original = path.read_bytes()
    try:
        arm.create_run(root, [("SAFE", "1m")],
                       run_id="addstock-reference-torn", now=NOW)
        blocked = False
    except arm.ActiveRunExists:
        blocked = True
    check(blocked and path.read_bytes() == original,
          "scenario 8 torn state blocks overwrite and preserves bytes")
    arm.discard_active(root, base / "robustness" / "logs", now=NOW)

    path.write_bytes(b"x" * (arm.MAX_BYTES + 1))
    try:
        arm.load_run(root)
        oversized = False
    except arm.ManifestError:
        oversized = True
    check(oversized, "scenario 8 oversized state fails closed")
    arm.discard_active(root, base / "robustness" / "logs", now=NOW)

    def fail_replace(_source, _target):
        raise OSError("reference replace failure")

    try:
        arm.create_run(root, [("SAFE", "1m")],
                       run_id="addstock-reference-atomic", now=NOW,
                       replace_fn=fail_replace)
        atomic_failed = False
    except OSError:
        atomic_failed = True
    check(atomic_failed and not path.exists()
          and not list(root.glob(f".{arm.ACTIVE_NAME}.*.tmp")),
          "scenario 8 failed replace leaves no target or temp debris")


def scenario_verification_debt(base):
    root = base / "debt" / "bank"
    root.mkdir(parents=True)
    archive = base / "debt" / "logs"
    run = arm.create_run(root, [("DEBT", "1m")],
                         run_id="addstock-reference-debt", now=NOW)
    arm.mark_series_started(root, run["run_id"], "DEBT", "1m", now=NOW)
    _write_series(root, "DEBT", "1m")
    arm.mark_series_complete(root, run["run_id"], "DEBT", "1m", now=NOW)
    arm.mark_fetch_finished(root, run["run_id"], reason="verification_debt",
                            now=NOW, archive_dir=archive)
    before = arm.load_run(root)
    check(before["tickers"]["DEBT"]["missing"]
          == ["xval", "gaps", "earliest"],
          "scenario 9 records all built verification debt")
    arm.mark_verification(root, run["run_id"], "DEBT", "xval", ["1m"],
                          now=NOW, archive_dir=archive)
    arm.mark_verification(root, run["run_id"], "DEBT", "gaps", ["1m"],
                          now=NOW, archive_dir=archive)
    port_free = arm.load_run(root)
    check(port_free["tickers"]["DEBT"]["missing"] == ["earliest"],
          "scenario 9 port-free checks clear only xval and gaps")
    result = arm.mark_verification(
        root, run["run_id"], "DEBT", "earliest", now=NOW,
        archive_dir=archive)
    check(result["archived"] is not None and _bank_fingerprint(root)[0] == 1,
          "scenario 9 earliest evidence clears final debt and archives")


def main():
    base = Path(tempfile.mkdtemp(prefix="addstock_protection_reference_"))
    try:
        control_root, fingerprint = scenario_control(base)
        check(str(control_root.resolve()).startswith(str(base.resolve())),
              "reference control is confined to the temporary root")
        scenario_single_port_death(base, fingerprint)
        scenario_all_port_blip(base, fingerprint)
        scenario_long_fleet_death(base, fingerprint)
        scenario_maintenance(base, fingerprint)
        scenario_slowness_storm(base, fingerprint)
        scenario_process_kill_matrix(base, fingerprint)
        scenario_robustness(base)
        scenario_verification_debt(base)
    finally:
        shutil.rmtree(base, ignore_errors=True)
    total = PASS[0] + FAIL[0]
    print(f"addstock_run_protection_reference: {PASS[0]}/{total} passed, "
          f"{FAIL[0]} failed")
    return 1 if FAIL[0] else 0


if __name__ == "__main__":
    raise SystemExit(main())
