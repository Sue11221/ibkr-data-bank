"""Offline contract checks for Row 51 M3 volatility reconciliation.

The suite owns every byte it reads or writes under ``TemporaryDirectory``.
The Operations owner supplies explicit children and wraps FakeAdapter payloads
as raw LiveIB transports. Actual hard-exit children establish their own intact
offline guard; no socket, TWS, GUI or production bank is used.

Run:
    python -B engine/vol_value_reconcile_selftest.py
"""

from __future__ import annotations

import datetime as dt
import hashlib
import importlib
import json
import os
import subprocess
import sys
import tempfile
import threading
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo


sys.path.insert(0, str(Path(__file__).resolve().parent))


def main() -> int:
    # Delegate before production imports; only the existing owner installs
    # transport tripwires and mints policy, including in real crash children.
    import unittest
    from fetch_a2_ibkr_workflows_selftest import Operations
    if len(sys.argv) == 4 and sys.argv[1] == "--crash-worker":
        case = Operations("legacy_ratio_crash_worker")
        case.crash_root, case.crash_phase = Path(sys.argv[2]), sys.argv[3]
        cases = [case]
    elif len(sys.argv) == 1:
        cases = [Operations("legacy_ratio_noncrash_contracts_through_confined_transport"),
                 Operations("legacy_ratio_hard_exit_contracts")]
    else:
        return 2
    result = unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite(cases))
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())


# The confined owner imports these scenarios inside its transport guard.
import market_calendar  # noqa: E402
import stock_ibkr as ibkr  # noqa: E402
import stock_storage as storage  # noqa: E402
import vol_value_audit as audit  # noqa: E402


FAILS: list[str] = []
CHECKS = 0
NY = ZoneInfo("America/New_York")


def check(name: str, condition, detail: str = "") -> None:
    global CHECKS
    CHECKS += 1
    ok = bool(condition)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}"
          + ("" if ok or not detail else f"  {detail}"))
    if not ok:
        FAILS.append(name)


def rejects(call) -> bool:
    try:
        call()
    except (audit.VolValueAuditError, RuntimeError, ValueError, TypeError):
        return True
    return False


def fresh_root(base: Path, name: str) -> Path:
    root = base / name / storage.STORAGE_DIR_NAME
    root.mkdir(parents=True)
    return root


def trading_days(first: dt.date, count: int) -> list[dt.date]:
    days: list[dt.date] = []
    day = first
    while len(days) < count:
        if market_calendar.is_trading_day(
                day, special_closures=frozenset()):
            days.append(day)
        day += dt.timedelta(days=1)
    return days


def day_bar(day: dt.date, value: float, *, minute: int = 15 * 60 + 59):
    stamp = dt.datetime.combine(day, dt.time(minute // 60, minute % 60))
    number = float(value)
    return (stamp, number, number, number, number, 0)


def write_values(root: Path, ticker: str, interval: str,
                 pairs: list[tuple[dt.date, float]], *,
                 conid: int | None = None,
                 fmt: str = "parquet") -> dict[str, Path]:
    """Replace fixture months and optionally publish a pinned manifest."""
    grouped: dict[tuple[int, int], list[tuple]] = {}
    for day, value in pairs:
        grouped.setdefault((day.year, day.month), []).append(
            day_bar(
                day, value,
                minute=(0 if storage.base_interval(interval).endswith("d")
                        else 15 * 60 + 59)))

    paths: dict[str, Path] = {}
    stats_by_month: dict[str, dict] = {}
    for (year, month), bars in sorted(grouped.items()):
        path = storage.month_file_path(
            root, ticker, year, month, interval, fmt=fmt)
        path.parent.mkdir(parents=True, exist_ok=True)
        stats = storage.write_month_file(path, sorted(bars))
        key = storage.month_key(year, month)
        paths[key] = path
        stats_by_month[key] = stats

    if conid is not None:
        manifest = storage.new_manifest(ticker, ticker)
        manifest["conid"] = int(conid)
        months = storage.manifest_months(manifest, interval)
        for key, stats in sorted(stats_by_month.items()):
            months[key] = dict(
                stats, status="present", source="vol-reconcile-selftest")
        storage.save_manifest(root / ticker, manifest)
    return paths


def file_sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def queue_bytes(root: Path) -> bytes:
    return (root / audit.QUEUE_BASENAME).read_bytes()


def queue_key(row: dict) -> tuple[str, str, str]:
    return row["ticker"], row["kind_token"], row["day"]


def result_key(result: dict) -> tuple[object, object, object]:
    return (
        result.get("ticker"),
        result.get("kind_token", result.get("kind")),
        result.get("day"),
    )


def run_fingerprint_checks() -> None:
    print("=== deterministic full-day evidence ==============================")
    day = dt.date(2024, 6, 18)
    bars = [
        day_bar(day, 0.3, minute=9 * 60 + 30),
        day_bar(day, 0.4),
    ]
    first = audit.day_fingerprint(bars)
    second = audit.day_fingerprint(list(bars))
    altered = list(bars)
    altered[0] = (
        altered[0][0], altered[0][1], altered[0][2] + 0.000001,
        altered[0][3], altered[0][4], altered[0][5],
    )
    changed = audit.day_fingerprint(altered)
    minus_zero = [(*bars[0][:-1], -0.0), bars[1]]
    signed_zero = audit.day_fingerprint(minus_zero)

    check("day fingerprint is deterministic and publishes its bar count",
          first == second and first["bars"] == 2
          and len(first["sha256"]) == 64,
          repr((first, second)))
    check("fingerprint covers intraday OHLC bits, not only final close",
          changed["sha256"] != first["sha256"],
          repr((first, changed)))
    check("fingerprint preserves exact IEEE signed-zero evidence",
          signed_zero["sha256"] != first["sha256"],
          repr((first, signed_zero)))
    check("empty, non-monotonic, aware, and subsecond days fail closed",
          all((
              rejects(lambda: audit.day_fingerprint([])),
              rejects(lambda: audit.day_fingerprint(list(reversed(bars)))),
              rejects(lambda: audit.day_fingerprint([
                  (bars[0][0].replace(tzinfo=dt.timezone.utc), *bars[0][1:])
              ])),
              rejects(lambda: audit.day_fingerprint([
                  (bars[0][0].replace(microsecond=1), *bars[0][1:])
              ])),
          )))


def run_queue_cas_checks(base: Path) -> None:
    print("=== validated queue snapshot + exact-row CAS =====================")
    root = fresh_root(base, "queue-cas")
    mon = dt.date(2024, 6, 17)
    tue = mon + dt.timedelta(days=1)
    for ticker in ("AAA", "BBB"):
        write_values(root, ticker, "1m-iv", [(mon, 0.1), (tue, 0.8)])
    audit.audit(
        root, write_queue=True,
        now=dt.datetime(2026, 7, 22, 12, tzinfo=dt.timezone.utc))

    rows, source = audit.queue_snapshot(root, include_source=True)
    check("queue_snapshot validates, sorts, and reports the durable source",
          [queue_key(row) for row in rows] == [
              ("AAA", "1m-iv", tue.isoformat()),
              ("BBB", "1m-iv", tue.isoformat()),
          ]
          and source["sha256"] == hashlib.sha256(queue_bytes(root)).hexdigest(),
          repr((rows, source)))
    filtered = audit.queue_snapshot(root, tickers=["bbb"])
    check("queue snapshot ticker filtering is canonical and exact",
          len(filtered) == 1 and filtered[0]["ticker"] == "BBB",
          repr(filtered))
    rows[0]["reasons"].append("caller-mutation")
    check("queue snapshots are detached from durable nested row state",
          "caller-mutation" not in audit.queue_snapshot(root)[0]["reasons"])
    check("text is rejected where a ticker iterable is required",
          rejects(lambda: audit.queue_snapshot(root, tickers="AAA")))

    old_aaa = next(
        row for row in audit.queue_snapshot(root) if row["ticker"] == "AAA")
    write_values(root, "AAA", "1m-iv", [(mon, 0.1), (tue, 0.9)])
    audit.audit(root, tickers=["AAA"], write_queue=True)
    stale_queue_before = queue_bytes(root)
    registry_path = root / audit.SETTLED_BASENAME
    registry_before = (
        registry_path.read_bytes() if registry_path.exists() else None)
    stale = audit.finalize_reconcile(
        root, old_aaa, action="settled", run="stale-cas-selftest",
        value=0.8, reason="jump", confirmed="2026-07-22")
    registry_after = (
        registry_path.read_bytes() if registry_path.exists() else None)
    check("stale expected row returns stale with byte-zero side effects",
          stale["status"] == "stale"
          and queue_bytes(root) == stale_queue_before
          and registry_after == registry_before,
          repr(stale))

    fresh_aaa = next(
        row for row in audit.queue_snapshot(root) if row["ticker"] == "AAA")
    corrected = audit.finalize_reconcile(
        root, fresh_aaa, action="corrected", run="corrected-cas-selftest")
    remaining = audit.queue_snapshot(root)
    check("corrected finalization retires only the exact current queue row",
          corrected["status"] == "corrected"
          and result_key(corrected) == queue_key(fresh_aaa)
          and [row["ticker"] for row in remaining] == ["BBB"],
          repr((corrected, remaining)))

    bbb = remaining[0]
    settled = audit.finalize_reconcile(
        root, bbb, action="settled", run="settled-cas-selftest",
        value=0.8, reason="jump", confirmed="2026-07-22")
    registry = json.loads(registry_path.read_text(encoding="utf-8"))
    check("scalar close settlement is registry-first and empties exact debt",
          settled["status"] == "settled"
          and not audit.queue_snapshot(root)
          and registry["BBB"]["1m-iv"][tue.isoformat()]["value"] == 0.8,
          repr((settled, registry)))


def run_full_day_settlement_checks(base: Path) -> None:
    print("=== strong refetch evidence + changed-day reflag =================")
    root = fresh_root(base, "full-day-evidence")
    prior = trading_days(dt.date(2024, 1, 2), 20)
    target = dt.date(2024, 2, 5)
    paths = write_values(root, "UNIT", "1m-iv", [
        *[(day, 0.1) for day in prior],
        (target, 5.0),
    ])
    report = audit.audit(root, write_queue=True)
    row = audit.queue_snapshot(root)[0]
    target_path = paths[storage.month_key(target.year, target.month)]
    target_bars, _stats = storage.read_month_file(target_path)
    target_bars = [bar for bar in target_bars if bar[0].date() == target]
    fingerprint = audit.day_fingerprint(target_bars)
    evidence = {
        "version": 1,
        "method": "ibkr_ratio_day_refetch",
        "what_to_show": "OPTION_IMPLIED_VOLATILITY",
        "stored_day_sha256": fingerprint["sha256"],
        "served_day_sha256": fingerprint["sha256"],
        "bars": fingerprint["bars"],
        "request_count": 1,
        "reasons": list(row["reasons"]),
        "month_sha256": file_sha(target_path),
    }
    check("fixture is an exact unit-flip finding with a stored-day fingerprint",
          row["reasons"] == ["unit_flip_month"]
          and report["flagged"] and fingerprint["bars"] == 1,
          repr((row, fingerprint)))

    bad_evidence = dict(evidence, served_day_sha256="f" * 64)
    before_bad = queue_bytes(root)
    bad_refused = rejects(lambda: audit.finalize_reconcile(
        root, row, action="settled", run="bad-evidence-selftest",
        value=5.0, reason="unit_flip_month", confirmed="2026-07-22",
        evidence=bad_evidence))
    check("mismatched stored/served evidence is rejected before any write",
          bad_refused and queue_bytes(root) == before_bad
          and not (root / audit.SETTLED_BASENAME).exists())

    multi_request_evidence = dict(evidence, request_count=2)
    multi_request_refused = rejects(lambda: audit.finalize_reconcile(
        root, row, action="settled", run="multi-request-evidence-selftest",
        value=5.0, reason="unit_flip_month", confirmed="2026-07-22",
        evidence=multi_request_evidence))
    check("strong settlement evidence requires exactly one source request",
          multi_request_refused and queue_bytes(root) == before_bad
          and not (root / audit.SETTLED_BASENAME).exists())

    settled = audit.finalize_reconcile(
        root, row, action="settled", run="strong-evidence-selftest",
        value=5.0, reason="unit_flip_month", confirmed="2026-07-22",
        evidence=evidence)
    same = audit.audit(root, write_queue=True)
    check("matching one-request full-day evidence suppresses unchanged unit flip",
          settled["status"] == "settled"
          and not audit.queue_snapshot(root)
          and not same["flagged"] and len(same["suppressed"]) == 1,
          repr((settled, same["flagged"], same["suppressed"])))

    storage.write_month_file(target_path, [
        day_bar(target, 0.2, minute=9 * 60 + 30),
        day_bar(target, 5.0),
    ])
    changed = audit.audit(root, write_queue=True)
    changed_rows = audit.queue_snapshot(root)
    check("same close with changed intraday content invalidates full-day evidence",
          len(changed_rows) == 1
          and changed_rows[0]["reasons"] == ["unit_flip_month"]
          and changed_rows[0]["value"] == 5.0
          and changed["flagged"] and not changed["suppressed"],
          repr((changed["flagged"], changed["suppressed"])))

    hard_root = fresh_root(base, "hard-values-cannot-settle")
    hard_day = dt.date(2024, 6, 18)
    hard_path = storage.month_file_path(
        hard_root, "HARD", hard_day.year, hard_day.month, "1m-iv")
    hard_path.parent.mkdir(parents=True, exist_ok=True)
    hard_bars = [(
        dt.datetime.combine(hard_day, dt.time(15, 59)),
        0.3, 11.0, 0.3, 0.3, 0,
    )]
    hard_path.write_bytes(storage._bars_to_parquet(hard_bars))
    audit.audit(hard_root, write_queue=True)
    hard_row = audit.queue_snapshot(hard_root)[0]
    hard_fingerprint = audit.day_fingerprint(hard_bars)
    hard_evidence = {
        "version": 1,
        "method": "ibkr_ratio_day_refetch",
        "what_to_show": "OPTION_IMPLIED_VOLATILITY",
        "stored_day_sha256": hard_fingerprint["sha256"],
        "served_day_sha256": hard_fingerprint["sha256"],
        "bars": 1,
        "request_count": 1,
        "reasons": ["hard_ceiling"],
        "month_sha256": file_sha(hard_path),
    }
    hard_before = queue_bytes(hard_root)
    hard_refused = rejects(lambda: audit.finalize_reconcile(
        hard_root, hard_row, action="settled", run="hard-selftest",
        value=0.3, reason="hard_ceiling", confirmed="2026-07-22",
        evidence=hard_evidence))
    check("hard-invalid stored bars require correction and can never settle",
          hard_refused and queue_bytes(hard_root) == hard_before
          and not (hard_root / audit.SETTLED_BASENAME).exists(),
          repr(hard_row))


class FakePacer:
    def __init__(self):
        self.calls: list[tuple[tuple, dict]] = []
        self.canonical_calls: list[tuple[tuple, dict]] = []

    def wait_turn(self, *args, **kwargs):
        self.calls.append((args, kwargs))


class FakeAdapter:
    def __init__(self, raw_bars, *, conid: int, on_fetch=None,
                 fetch_error=None):
        self.raw_bars = list(raw_bars)
        self.conid = int(conid)
        self.on_fetch = on_fetch
        self.fetch_error = fetch_error
        self.use_rth = False
        self.fetch_calls: list[dict] = []
        self.contract_calls: list[int] = []
        self.disconnect_calls = 0

    def contract_for(self, conid):
        self.contract_calls.append(int(conid))
        return SimpleNamespace(conId=int(conid), symbol="FIXTURE")

    def account(self):
        return "DU-ROW51-SELFTEST"

    def is_connected(self):
        return True

    def reconnect(self):
        raise AssertionError("offline fixture must not reconnect")

    def disconnect(self):
        self.disconnect_calls += 1

    def fetch(self, contract, end_dt, duration, bar_size,
              what_to_show="TRADES"):
        self.fetch_calls.append({
            "conid": getattr(contract, "conId", None),
            "end": end_dt,
            "duration": duration,
            "bar_size": bar_size,
            "what_to_show": what_to_show,
            "use_rth": self.use_rth,
        })
        if self.on_fetch is not None:
            self.on_fetch()
        if self.fetch_error is not None:
            raise self.fetch_error
        return list(self.raw_bars)


class EndlessAdapter(FakeAdapter):
    """Faulty source used to prove one-day materialization is tightly bound."""

    def __init__(self, day: dt.date, *, conid: int):
        super().__init__([], conid=conid)
        self.day = day
        self.produced = 0

    def fetch(self, contract, end_dt, duration, bar_size,
              what_to_show="TRADES"):
        self.fetch_calls.append({
            "conid": getattr(contract, "conId", None),
            "end": end_dt,
            "duration": duration,
            "bar_size": bar_size,
            "what_to_show": what_to_show,
            "use_rth": self.use_rth,
        })

        def rows():
            while True:
                self.produced += 1
                yield vendor_bar(self.day, 0.8)

        return rows()


def vendor_bar(day: dt.date, value: float, *, daily: bool = False,
               minute: int = 15 * 60 + 59):
    stamp = day if daily else (
        dt.datetime.combine(
            day, dt.time(minute // 60, minute % 60), tzinfo=NY)
        .astimezone(dt.timezone.utc))
    number = float(value)
    return SimpleNamespace(
        date=stamp, open=number, high=number, low=number, close=number,
        volume=1.0)


def month_values(root: Path, ticker: str, interval: str) -> dict[dt.date, float]:
    values: dict[dt.date, float] = {}
    for path in sorted((root / ticker).rglob(f"*_{interval}.parquet")):
        bars, _stats = storage.read_month_file(path)
        for bar in bars:
            values[bar[0].date()] = float(bar[4])
    return values


def run_shared_reconciler_checks(base: Path) -> None:
    print("=== shared fake-adapter reconcile path ===========================")
    try:
        reconcile = importlib.import_module("vol_value_reconcile")
    except ModuleNotFoundError as exc:
        check("shared vol_value_reconcile module is importable", False, str(exc))
        return

    check("VOL_VALUE_RECONCILE ships enabled",
          getattr(reconcile, "VOL_VALUE_RECONCILE", None) is True,
          repr(getattr(reconcile, "VOL_VALUE_RECONCILE", None)))

    mon = dt.date(2024, 6, 17)
    tue = mon + dt.timedelta(days=1)

    settle_root = fresh_root(base, "shared-settle")
    settle_paths = write_values(
        settle_root, "SAME", "1m-iv", [(mon, 0.1), (tue, 0.8)],
        conid=91001)
    audit.audit(settle_root, write_queue=True)
    planned = list(reconcile.plan(
        settle_root, tickers=["same"], kinds=["1m-iv"]))
    excluded = list(reconcile.plan(
        settle_root, tickers=["same"], kinds=["1d-hvol"]))
    row = planned[0] if planned else {}
    same_path = settle_paths[storage.month_key(mon.year, mon.month)]
    same_before = file_sha(same_path)
    same_adapter = FakeAdapter([vendor_bar(tue, 0.8)], conid=91001)
    same_pacer = FakePacer()
    same = reconcile.reconcile_one(
        same_adapter, same_pacer, settle_root, row,
        run_id="shared-same-selftest")
    check("plan applies exact ticker/kind filters to durable queue debt",
          len(planned) == 1 and queue_key(row)
          == ("SAME", "1m-iv", tue.isoformat()) and not excluded,
          repr((planned, excluded)))
    check("same served day settles after exactly one pinned, kind-specific RTH request",
          same.get("status") == "settled"
          and result_key(same) == queue_key(row)
          and same.get("request_count") == 1
          and len(same_adapter.fetch_calls) == len(same_pacer.canonical_calls) == 1
          and not same_pacer.calls
          and same_adapter.contract_calls == [91001]
          and same_adapter.fetch_calls[0]["what_to_show"]
          == "OPTION_IMPLIED_VOLATILITY"
          and same_adapter.fetch_calls[0]["use_rth"] is True
          and same_pacer.canonical_calls[0][1].get("metered") is False
          and same_adapter.use_rth is False,
          repr((same, same_adapter.fetch_calls, same_pacer.calls)))
    check("settlement preserves the month and retires its durable queue row",
          file_sha(same_path) == same_before
          and not audit.queue_snapshot(settle_root)
          and len(audit.settled_for_export(
              settle_root, [("SAME", "1m-iv")])) == 1)

    registry_fail_root = fresh_root(base, "shared-settle-registry-failure")
    write_values(
        registry_fail_root, "REGFAIL", "1m-iv",
        [(mon, 0.1), (tue, 0.8)], conid=91031)
    audit.audit(registry_fail_root, write_queue=True)
    registry_fail_row = list(reconcile.plan(registry_fail_root))[0]
    original_atomic_write = storage._atomic_write_bytes

    def fail_registry_write(target, payload):
        if Path(target).name == audit.SETTLED_BASENAME:
            raise storage.StorageError("forced settlement registry failure")
        return original_atomic_write(target, payload)

    storage._atomic_write_bytes = fail_registry_write
    try:
        registry_fail_result = reconcile.reconcile_one(
            FakeAdapter([vendor_bar(tue, 0.8)], conid=91031),
            FakePacer(), registry_fail_root, registry_fail_row,
            run_id="shared-settle-registry-failure-selftest")
    finally:
        storage._atomic_write_bytes = original_atomic_write
    check("registry write failure remains unresolved with exact durable debt",
          registry_fail_result.get("status") == "unresolved"
          and registry_fail_result.get("queue_resolved") is False
          and not registry_fail_result.get("registry_committed", False)
          and audit.queue_snapshot(registry_fail_root) == [registry_fail_row]
          and not (registry_fail_root / audit.SETTLED_BASENAME).exists(),
          repr(registry_fail_result))

    queue_fail_root = fresh_root(base, "shared-settle-queue-failure")
    write_values(
        queue_fail_root, "QUEUEFAIL", "1m-iv",
        [(mon, 0.1), (tue, 0.8)], conid=91032)
    audit.audit(queue_fail_root, write_queue=True)
    queue_fail_row = list(reconcile.plan(queue_fail_root))[0]
    queue_writes_to_skip = 0

    def fail_queue_write(target, payload):
        nonlocal queue_writes_to_skip
        if Path(target).name == audit.QUEUE_BASENAME:
            if queue_writes_to_skip:
                queue_writes_to_skip -= 1
                return original_atomic_write(target, payload)
            raise storage.StorageError("forced settlement queue failure")
        return original_atomic_write(target, payload)

    def install_queue_write_failure():
        nonlocal queue_writes_to_skip
        # The mandatory post-fetch focused audit publishes the first queue;
        # fail the subsequent exact-row finalization publication instead.
        queue_writes_to_skip = 1
        storage._atomic_write_bytes = fail_queue_write

    try:
        queue_fail_result = reconcile.reconcile_one(
            FakeAdapter(
                [vendor_bar(tue, 0.8)], conid=91032,
                on_fetch=install_queue_write_failure),
            FakePacer(), queue_fail_root, queue_fail_row,
            run_id="shared-settle-queue-failure-selftest")
    finally:
        storage._atomic_write_bytes = original_atomic_write
    queue_fail_registry = json.loads(
        (queue_fail_root / audit.SETTLED_BASENAME).read_text(
            encoding="utf-8"))
    queue_fail_ledger = reconcile.ledger([queue_fail_result])
    check("durable settlement stays settled when queue retirement fails",
          queue_fail_result.get("status") == "settled"
          and queue_fail_result.get("registry_committed") is True
          and queue_fail_result.get("queue_resolved") is False
          and audit.queue_snapshot(queue_fail_root) == [queue_fail_row]
          and queue_fail_registry["QUEUEFAIL"]["1m-iv"][
              tue.isoformat()]["value"] == 0.8
          and queue_fail_ledger["settled_count"] == 1
          and queue_fail_ledger["unresolved_count"] == 0
          and queue_fail_ledger["queue_pending_count"] == 1,
          repr((queue_fail_result, queue_fail_ledger)))

    release_fail_root = fresh_root(base, "shared-settle-release-failure")
    write_values(
        release_fail_root, "RELFAIL", "1m-iv",
        [(mon, 0.1), (tue, 0.8)], conid=91033)
    audit.audit(release_fail_root, write_queue=True)
    release_fail_row = list(reconcile.plan(release_fail_root))[0]
    original_transaction = audit._sidecar_transaction
    sidecar_transactions_to_skip = 0

    @contextmanager
    def fail_after_transaction(root):
        nonlocal sidecar_transactions_to_skip
        with original_transaction(root):
            yield
        if sidecar_transactions_to_skip:
            sidecar_transactions_to_skip -= 1
            return
        raise audit.VolValueAuditError("forced post-write release failure")

    def install_sidecar_release_failure():
        nonlocal sidecar_transactions_to_skip
        sidecar_transactions_to_skip = 1
        audit._sidecar_transaction = fail_after_transaction

    try:
        release_fail_result = reconcile.reconcile_one(
            FakeAdapter(
                [vendor_bar(tue, 0.8)], conid=91033,
                on_fetch=install_sidecar_release_failure),
            FakePacer(), release_fail_root, release_fail_row,
            run_id="shared-settle-release-failure-selftest")
    finally:
        audit._sidecar_transaction = original_transaction
    check("post-write cleanup failure preserves both completed settlement stages",
          release_fail_result.get("status") == "settled"
          and release_fail_result.get("registry_committed") is True
          and release_fail_result.get("queue_resolved") is True
          and not audit.queue_snapshot(release_fail_root)
          and len(audit.settled_for_export(
              release_fail_root, [("RELFAIL", "1m-iv")])) == 1,
          repr(release_fail_result))

    hvol_root = fresh_root(base, "shared-hvol-kind")
    write_values(
        hvol_root, "HV", "1d-hvol", [(mon, 0.1), (tue, 0.8)],
        conid=91006)
    audit.audit(hvol_root, write_queue=True)
    hvol_row = list(reconcile.plan(hvol_root))[0]
    hvol_adapter = FakeAdapter(
        [vendor_bar(tue, 0.8, daily=True)], conid=91006)
    hvol_result = reconcile.reconcile_one(
        hvol_adapter, FakePacer(), hvol_root, hvol_row,
        run_id="shared-hvol-selftest")
    check("HVOL replay uses its own kind-specific one-day request",
          hvol_result.get("status") == "settled"
          and hvol_result.get("request_count") == 1
          and len(hvol_adapter.fetch_calls) == 1
          and hvol_adapter.fetch_calls[0]["what_to_show"]
          == "HISTORICAL_VOLATILITY"
          and not audit.queue_snapshot(hvol_root),
          repr((hvol_result, hvol_adapter.fetch_calls)))

    correct_root = fresh_root(base, "shared-correct")
    correct_paths = write_values(
        correct_root, "DIFF", "1m-iv", [(mon, 0.1), (tue, 0.8)],
        conid=91002)
    audit.audit(correct_root, write_queue=True)
    correct_row = list(reconcile.plan(correct_root))[0]
    correct_path = correct_paths[storage.month_key(mon.year, mon.month)]
    correct_before = file_sha(correct_path)
    correct_adapter = FakeAdapter([vendor_bar(tue, 0.7)], conid=91002)
    corrected = reconcile.reconcile_one(
        correct_adapter, FakePacer(), correct_root, correct_row,
        run_id="shared-different-selftest")
    corrected_manifest = storage.load_manifest(correct_root / "DIFF") or {}
    corrected_month = (((corrected_manifest.get("intervals") or {})
                        .get("1m-iv") or {}).get("months") or {}).get(
                            storage.month_key(mon.year, mon.month), {})
    corrections = corrected_month.get("value_corrections") or []
    check("different served day replaces only that day and clears corrected debt",
          corrected.get("status") == "corrected"
          and result_key(corrected) == queue_key(correct_row)
          and corrected.get("reason") == correct_row.get("reason")
          and corrected.get("reasons") == correct_row.get("reasons")
          and corrected.get("request_count") == 1
          and month_values(correct_root, "DIFF", "1m-iv")
          == {mon: 0.1, tue: 0.7}
          and file_sha(correct_path) != correct_before
          and not audit.queue_snapshot(correct_root),
          repr((corrected, month_values(correct_root, "DIFF", "1m-iv"))))
    check("replacement publishes verified month SHA and bounded correction evidence",
          corrected_month.get("sha256") == file_sha(correct_path)
          and any(item.get("type") == "vol_value_refetch_reconcile"
                  and item.get("day") == tue.isoformat()
                  and item.get("reason") == correct_row.get("reason")
                  and item.get("reasons") == correct_row.get("reasons")
                  for item in corrections),
          repr(corrected_month))

    cross_root = fresh_root(base, "shared-cross-kind-context-refresh")
    write_values(
        cross_root, "CROSS", "1m-iv",
        [(mon, 0.1), (tue, 0.8)], conid=91044)
    cross_hvol_paths = write_values(
        cross_root, "CROSS", "1d-hvol",
        [(mon, 0.1), (tue, 0.02)])
    cross_manifest = storage.load_manifest(cross_root / "CROSS") or {}
    cross_hvol_months = storage.manifest_months(
        cross_manifest, "1d-hvol")
    for month_key, path in cross_hvol_paths.items():
        _bars, stats = storage.read_month_file(path)
        cross_hvol_months[month_key] = dict(
            stats, status="present", source="vol-reconcile-selftest")
    storage.save_manifest(cross_root / "CROSS", cross_manifest)
    audit.audit(cross_root, write_queue=True)
    cross_rows = list(reconcile.plan(cross_root))
    cross_iv_row = next(
        item for item in cross_rows if item["kind_token"] == "1m-iv")
    cross_hvol_row = next(
        item for item in cross_rows if item["kind_token"] == "1d-hvol")
    cross_lock = threading.Lock()
    cross_iv_adapter = FakeAdapter(
        [vendor_bar(tue, 0.3)], conid=91044)
    cross_iv_result = reconcile.reconcile_one(
        cross_iv_adapter, FakePacer(), cross_root, cross_iv_row,
        run_id="shared-cross-kind-first-selftest",
        manifest_lock=cross_lock)
    cross_after_first = list(audit.queue_snapshot(cross_root))
    cross_hvol_adapter = FakeAdapter(
        [vendor_bar(tue, 0.02, daily=True)], conid=91044)
    cross_hvol_result = reconcile.reconcile_one(
        cross_hvol_adapter, FakePacer(), cross_root, cross_hvol_row,
        run_id="shared-cross-kind-second-selftest",
        manifest_lock=cross_lock)
    check("focused re-audit prevents a vanished IV/HVOL row source request",
          len(cross_rows) == 2
          and cross_iv_result.get("status") == "corrected"
          and cross_iv_result.get("request_count") == 1
          and cross_after_first == [cross_hvol_row]
          and cross_hvol_result.get("status") == "stale"
          and cross_hvol_result.get("request_count") == 0
          and not cross_hvol_adapter.fetch_calls
          and not audit.queue_snapshot(cross_root),
          repr((cross_rows, cross_iv_result, cross_after_first,
                cross_hvol_result, cross_hvol_adapter.fetch_calls)))

    inflight_root = fresh_root(base, "shared-cross-kind-inflight-drift")
    inflight_iv_paths = write_values(
        inflight_root, "INFLIGHT", "1m-iv",
        [(mon, 0.1), (tue, 0.8)], conid=91047)
    inflight_hvol_paths = write_values(
        inflight_root, "INFLIGHT", "1d-hvol",
        [(mon, 0.1), (tue, 0.02)])
    inflight_manifest = storage.load_manifest(
        inflight_root / "INFLIGHT") or {}
    inflight_hvol_months = storage.manifest_months(
        inflight_manifest, "1d-hvol")
    for month_key, path in inflight_hvol_paths.items():
        _bars, stats = storage.read_month_file(path)
        inflight_hvol_months[month_key] = dict(
            stats, status="present", source="vol-reconcile-selftest")
    storage.save_manifest(inflight_root / "INFLIGHT", inflight_manifest)
    audit.audit(inflight_root, write_queue=True)
    inflight_row = next(
        item for item in reconcile.plan(inflight_root)
        if item["kind_token"] == "1d-hvol")
    inflight_hvol_path = inflight_hvol_paths[
        storage.month_key(mon.year, mon.month)]
    inflight_hvol_before = inflight_hvol_path.read_bytes()

    def resolve_cross_kind_during_fetch():
        iv_path = inflight_iv_paths[storage.month_key(mon.year, mon.month)]
        iv_stats = storage.write_month_file(iv_path, [
            day_bar(mon, 0.1), day_bar(tue, 0.3),
        ])
        manifest = storage.load_manifest(inflight_root / "INFLIGHT") or {}
        entry = (((manifest.get("intervals") or {}).get("1m-iv") or {})
                 .get("months") or {}).get(
                     storage.month_key(mon.year, mon.month))
        entry.update(iv_stats)
        storage.save_manifest(inflight_root / "INFLIGHT", manifest)

    inflight_adapter = FakeAdapter(
        [vendor_bar(tue, 0.02, daily=True)], conid=91047,
        on_fetch=resolve_cross_kind_during_fetch)
    inflight_result = reconcile.reconcile_one(
        inflight_adapter, FakePacer(), inflight_root, inflight_row,
        run_id="shared-cross-kind-inflight-selftest",
        manifest_lock=threading.Lock())
    check("post-fetch focused audit blocks stale cross-kind settlement",
          inflight_result.get("status") == "stale"
          and inflight_result.get("request_count") == 1
          and len(inflight_adapter.fetch_calls) == 1
          and inflight_hvol_path.read_bytes() == inflight_hvol_before
          and not (inflight_root / audit.SETTLED_BASENAME).exists()
          and not audit.queue_snapshot(inflight_root),
          repr(inflight_result))

    incomplete_root = fresh_root(base, "shared-incomplete-focused-audit")
    write_values(
        incomplete_root, "INCOMPLETE", "1m-iv",
        [(mon, 0.1), (tue, 0.8)], conid=91045)
    audit.audit(incomplete_root, write_queue=True)
    incomplete_row = list(reconcile.plan(incomplete_root))[0]
    original_audit = audit.audit

    def incomplete_focused_audit(*args, **kwargs):
        report = dict(original_audit(*args, **kwargs))
        report["complete"] = False
        return report

    incomplete_adapter = FakeAdapter(
        [vendor_bar(tue, 0.8)], conid=91045)
    audit.audit = incomplete_focused_audit
    try:
        incomplete_result = reconcile.reconcile_one(
            incomplete_adapter, FakePacer(), incomplete_root, incomplete_row,
            run_id="shared-incomplete-focused-audit-selftest")
    finally:
        audit.audit = original_audit
    check("incomplete focused audit fails closed before a source request",
          incomplete_result.get("status") == "unresolved"
          and incomplete_result.get("request_count") == 0
          and "focused volatility audit is incomplete"
          in incomplete_result.get("error", "")
          and not incomplete_adapter.fetch_calls
          and audit.queue_snapshot(incomplete_root) == [incomplete_row],
          repr(incomplete_result))

    grid_root = fresh_root(base, "shared-complete-grid")
    grid_paths = write_values(
        grid_root, "GRID", "1m-iv", [(mon, 0.1), (tue, 0.8)],
        conid=91035)
    grid_path = grid_paths[storage.month_key(mon.year, mon.month)]
    grid_stats = storage.write_month_file(grid_path, [
        day_bar(mon, 0.1),
        day_bar(tue, 0.7, minute=9 * 60 + 30),
        day_bar(tue, 0.8),
    ])
    grid_manifest = storage.load_manifest(grid_root / "GRID") or {}
    grid_entry = ((((grid_manifest.get("intervals") or {})
                    .get("1m-iv") or {}).get("months") or {}).get(
                        storage.month_key(mon.year, mon.month)))
    grid_entry.update(grid_stats)
    storage.save_manifest(grid_root / "GRID", grid_manifest)
    audit.audit(grid_root, write_queue=True)
    grid_row = next(
        item for item in reconcile.plan(grid_root)
        if item["day"] == tue.isoformat())
    grid_before = (
        grid_path.read_bytes(),
        (grid_root / "GRID" / storage.MANIFEST_NAME).read_bytes(),
    )
    partial_grid = reconcile.reconcile_one(
        FakeAdapter([vendor_bar(tue, 0.8)], conid=91035),
        FakePacer(), grid_root, grid_row,
        run_id="shared-partial-grid-selftest")
    wrong_grid = reconcile.reconcile_one(
        FakeAdapter([
            vendor_bar(tue, 0.7, minute=9 * 60 + 31),
            vendor_bar(tue, 0.7),
        ], conid=91035), FakePacer(), grid_root, grid_row,
        run_id="shared-wrong-grid-selftest")
    grid_after_refusals = (
        grid_path.read_bytes(),
        (grid_root / "GRID" / storage.MANIFEST_NAME).read_bytes(),
    )
    grid_queue_after_refusals = list(audit.queue_snapshot(grid_root))
    exact_grid = reconcile.reconcile_one(
        FakeAdapter([
            vendor_bar(tue, 0.7, minute=9 * 60 + 30),
            vendor_bar(tue, 0.7),
        ], conid=91035), FakePacer(), grid_root, grid_row,
        run_id="shared-exact-grid-selftest")
    exact_grid_bars, _exact_grid_stats = storage.read_month_file(grid_path)
    check("partial and shifted refetch grids cannot settle or replace a full day",
          partial_grid.get("status") == "unresolved"
          and partial_grid.get("request_count") == 1
          and wrong_grid.get("status") == "unresolved"
          and wrong_grid.get("request_count") == 1
          and grid_after_refusals == grid_before
          and grid_queue_after_refusals == [grid_row],
          repr((partial_grid, wrong_grid)))
    check("an exact timestamp grid permits the guarded day correction",
          exact_grid.get("status") == "corrected"
          and exact_grid.get("request_count") == 1
          and len([bar for bar in exact_grid_bars
                   if bar[0].date() == tue]) == 2
          and all(float(bar[4]) == 0.7 for bar in exact_grid_bars
                  if bar[0].date() == tue)
          and not audit.queue_snapshot(grid_root),
          repr(exact_grid))

    correction_queue_fail_root = fresh_root(
        base, "shared-correction-queue-failure")
    write_values(
        correction_queue_fail_root, "CORRQFAIL", "1m-iv",
        [(mon, 0.1), (tue, 0.8)], conid=91034)
    audit.audit(correction_queue_fail_root, write_queue=True)
    correction_queue_fail_row = list(
        reconcile.plan(correction_queue_fail_root))[0]
    try:
        correction_queue_fail_result = reconcile.reconcile_one(
            FakeAdapter(
                [vendor_bar(tue, 0.7)], conid=91034,
                on_fetch=install_queue_write_failure),
            FakePacer(), correction_queue_fail_root,
            correction_queue_fail_row,
            run_id="shared-correction-queue-failure-selftest")
    finally:
        storage._atomic_write_bytes = original_atomic_write
    correction_queue_ledger = reconcile.ledger([
        correction_queue_fail_result])
    correction_queue_manifest = storage.load_manifest(
        correction_queue_fail_root / "CORRQFAIL") or {}
    correction_queue_month = (((
        correction_queue_manifest.get("intervals") or {}).get("1m-iv")
        or {}).get("months") or {}).get(
            storage.month_key(mon.year, mon.month), {})
    check("durable correction remains corrected when queue retirement fails",
          correction_queue_fail_result.get("status") == "corrected"
          and correction_queue_fail_result.get("bank_committed") is True
          and correction_queue_fail_result.get("queue_resolved") is False
          and audit.queue_snapshot(correction_queue_fail_root)
          == [correction_queue_fail_row]
          and month_values(
              correction_queue_fail_root, "CORRQFAIL", "1m-iv")
          == {mon: 0.1, tue: 0.7}
          and bool(correction_queue_month.get("value_corrections"))
          and correction_queue_ledger["corrected_count"] == 1
          and correction_queue_ledger["queue_pending_count"] == 1,
          repr((correction_queue_fail_result, correction_queue_ledger)))

    # A month write is not a correction until its manifest ledger verifies.
    # Exercise both publication failures against exact pre-operation bytes.
    save_fail_root = fresh_root(base, "shared-manifest-save-rollback")
    save_fail_paths = write_values(
        save_fail_root, "SAVEFAIL", "1m-iv",
        [(mon, 0.1), (tue, 0.8)], conid=91021, fmt="csv")
    audit.audit(save_fail_root, write_queue=True)
    save_fail_row = list(reconcile.plan(save_fail_root))[0]
    save_fail_path = save_fail_paths[storage.month_key(mon.year, mon.month)]
    save_fail_manifest = save_fail_root / "SAVEFAIL" / storage.MANIFEST_NAME
    save_fail_parquet = storage.month_file_path(
        save_fail_root, "SAVEFAIL", mon.year, mon.month, "1m-iv")
    save_fail_before = (
        save_fail_path.read_bytes(), save_fail_manifest.read_bytes())
    original_atomic_write = storage._atomic_write_bytes

    def forced_authoritative_manifest_failure(target, payload):
        if Path(target) == save_fail_manifest:
            raise storage.StorageError("forced authoritative manifest failure")
        return original_atomic_write(target, payload)

    storage._atomic_write_bytes = forced_authoritative_manifest_failure
    try:
        save_fail_result = reconcile.reconcile_one(
            FakeAdapter([vendor_bar(tue, 0.7)], conid=91021),
            FakePacer(), save_fail_root, save_fail_row,
            run_id="shared-manifest-save-rollback-selftest")
    finally:
        storage._atomic_write_bytes = original_atomic_write
    save_fail_after = (
        save_fail_path.read_bytes(), save_fail_manifest.read_bytes())
    save_fail_ledger = reconcile.ledger([save_fail_result])
    check("manifest save failure exactly restores CSV month/manifest and keeps debt",
          save_fail_result.get("status") == "unresolved"
          and save_fail_result.get("commit_state") == "rolled_back"
          and save_fail_result.get("rollback_verified") is True
          and save_fail_result.get("bank_write_completed") is False
          and save_fail_result.get("bank_written") is False
          and save_fail_result.get("bank_committed") is False
          and save_fail_result.get("correction_recorded") is False
          and save_fail_result.get("reason") == save_fail_row.get("reason")
          and save_fail_result.get("reasons") == save_fail_row.get("reasons")
          and save_fail_after == save_fail_before
          and audit.queue_snapshot(save_fail_root) == [save_fail_row]
          and not save_fail_parquet.exists()
          and save_fail_ledger["corrected_count"] == 0
          and save_fail_ledger["queue_pending_count"] == 1,
          repr((save_fail_result, save_fail_ledger)))

    verify_fail_root = fresh_root(base, "shared-manifest-verify-rollback")
    verify_fail_paths = write_values(
        verify_fail_root, "VERIFYFAIL", "1m-iv",
        [(mon, 0.1), (tue, 0.8)], conid=91022)
    audit.audit(verify_fail_root, write_queue=True)
    verify_fail_row = list(reconcile.plan(verify_fail_root))[0]
    verify_fail_path = verify_fail_paths[storage.month_key(mon.year, mon.month)]
    verify_fail_manifest = verify_fail_root / "VERIFYFAIL" / storage.MANIFEST_NAME
    verify_fail_before = (
        verify_fail_path.read_bytes(), verify_fail_manifest.read_bytes())
    original_strict_load = reconcile.bank._load_manifest_strict
    strict_load_calls = 0

    def forced_verification_failure(ticker_dir, ticker):
        nonlocal strict_load_calls
        strict_load_calls += 1
        value = original_strict_load(ticker_dir, ticker)
        if strict_load_calls == 3:
            value = json.loads(json.dumps(value))
            entry = (((value.get("intervals") or {}).get("1m-iv") or {})
                     .get("months") or {}).get(
                         storage.month_key(mon.year, mon.month)) or {}
            entry["value_corrections"] = []
        return value

    reconcile.bank._load_manifest_strict = forced_verification_failure
    try:
        verify_fail_result = reconcile.reconcile_one(
            FakeAdapter([vendor_bar(tue, 0.7)], conid=91022),
            FakePacer(), verify_fail_root, verify_fail_row,
            run_id="shared-manifest-verify-rollback-selftest")
    finally:
        reconcile.bank._load_manifest_strict = original_strict_load
    verify_fail_after = (
        verify_fail_path.read_bytes(), verify_fail_manifest.read_bytes())
    verify_fail_ledger = reconcile.ledger([verify_fail_result])
    check("manifest verification failure exactly rolls back with no false correction",
          strict_load_calls == 3
          and verify_fail_result.get("status") == "unresolved"
          and verify_fail_result.get("commit_state") == "rolled_back"
          and verify_fail_result.get("rollback_verified") is True
          and verify_fail_result.get("bank_written") is False
          and verify_fail_result.get("correction_recorded") is False
          and verify_fail_after == verify_fail_before
          and audit.queue_snapshot(verify_fail_root) == [verify_fail_row]
          and verify_fail_ledger["corrected_count"] == 0
          and verify_fail_ledger["queue_pending_count"] == 1,
          repr((verify_fail_result, verify_fail_ledger)))

    unproven_root = fresh_root(base, "shared-unproven-replacement-rollback")
    unproven_paths = write_values(
        unproven_root, "UNPROVEN", "1m-iv",
        [(mon, 0.1), (tue, 0.8)], conid=91048, fmt="csv")
    audit.audit(unproven_root, write_queue=True)
    unproven_row = list(reconcile.plan(unproven_root))[0]
    unproven_csv = unproven_paths[storage.month_key(mon.year, mon.month)]
    unproven_csv_before = unproven_csv.read_bytes()
    unproven_parquet = storage.month_file_path(
        unproven_root, "UNPROVEN", mon.year, mon.month, "1m-iv")
    unproven_payload = storage._bars_to_parquet([
        day_bar(mon, 0.1), day_bar(tue, 0.66),
    ])
    original_publish_month = reconcile.bank._publish_prepared_month

    def plant_unproven_replacement_then_fail(
            _staged_path, write_path, _expected_sha,
            _expected_before_sha):
        storage._atomic_write_bytes(write_path, unproven_payload)
        raise reconcile.bank.RatioDayError(
            "forced failure with an unproven replacement")

    reconcile.bank._publish_prepared_month = (
        plant_unproven_replacement_then_fail)
    try:
        unproven_result = reconcile.reconcile_one(
            FakeAdapter([vendor_bar(tue, 0.7)], conid=91048),
            FakePacer(), unproven_root, unproven_row,
            run_id="shared-unproven-replacement-rollback-selftest")
    finally:
        reconcile.bank._publish_prepared_month = original_publish_month
    unproven_stage = (unproven_root / "UNPROVEN" /
                      storage.VOL_VALUE_RECONCILE_STAGE_DIR)
    unproven_errors = ((unproven_result.get("commit_evidence") or {})
                       .get("restore_errors") or [])
    check("catchable rollback never deletes an unproven replacement path",
          unproven_result.get("status") == "ambiguous"
          and unproven_result.get("commit_state") == "recovery_required"
          and unproven_result.get("rollback_verified") is False
          and unproven_csv.read_bytes() == unproven_csv_before
          and unproven_parquet.read_bytes() == unproven_payload
          and unproven_stage.is_dir()
          and any("unproven bytes were not removed" in item
                  for item in unproven_errors)
          and audit.queue_snapshot(unproven_root) == [unproven_row],
          repr((unproven_result, unproven_errors)))

    uncertain_root = fresh_root(base, "shared-unverified-rollback")
    uncertain_paths = write_values(
        uncertain_root, "UNCERTAIN", "1m-iv",
        [(mon, 0.1), (tue, 0.8)], conid=91023)
    audit.audit(uncertain_root, write_queue=True)
    uncertain_row = list(reconcile.plan(uncertain_root))[0]
    uncertain_path = uncertain_paths[storage.month_key(mon.year, mon.month)]
    uncertain_manifest = uncertain_root / "UNCERTAIN" / storage.MANIFEST_NAME
    uncertain_month_before = uncertain_path.read_bytes()
    uncertain_manifest_before = uncertain_manifest.read_bytes()
    uncertain_queue_before = queue_bytes(uncertain_root)
    uncertain_old_sha = hashlib.sha256(uncertain_month_before).hexdigest()
    original_publish_month = reconcile.bank._publish_prepared_month

    def refuse_old_month_restore(target, payload):
        if (Path(target) == uncertain_path
                and hashlib.sha256(payload).hexdigest() == uncertain_old_sha):
            return None
        return original_atomic_write(target, payload)

    def publish_then_fail(staged_path, write_path, expected_sha,
                          expected_before_sha):
        original_publish_month(
            staged_path, write_path, expected_sha, expected_before_sha)
        raise reconcile.bank.RatioDayError(
            "forced failure after canonical month promotion")

    reconcile.bank._publish_prepared_month = publish_then_fail
    storage._atomic_write_bytes = refuse_old_month_restore
    try:
        uncertain_result = reconcile.reconcile_one(
            FakeAdapter([vendor_bar(tue, 0.7)], conid=91023),
            FakePacer(), uncertain_root, uncertain_row,
            run_id="shared-unverified-rollback-selftest")
    finally:
        storage._atomic_write_bytes = original_atomic_write
        reconcile.bank._publish_prepared_month = original_publish_month
    uncertain_ledger = reconcile.ledger([uncertain_result])
    uncertain_stage = (
        uncertain_root / "UNCERTAIN" /
        storage.VOL_VALUE_RECONCILE_STAGE_DIR)
    uncertain_audit = audit.audit(uncertain_root, write_queue=True)
    uncertain_queue_during_recovery = list(
        audit.queue_snapshot(uncertain_root))
    check("unverified rollback is explicit ambiguous recovery debt with truthful state",
          uncertain_result.get("status") == "ambiguous"
          and uncertain_result.get("commit_state") == "recovery_required"
          and uncertain_result.get("rollback_verified") is False
          and uncertain_result.get("bank_write_completed") is True
          and uncertain_result.get("bank_written") is True
          and uncertain_result.get("bank_committed") is True
          and uncertain_result.get("correction_recorded") is True
          and uncertain_path.read_bytes() != uncertain_month_before
          and uncertain_manifest.read_bytes() != uncertain_manifest_before
          and uncertain_queue_during_recovery == [uncertain_row]
          and uncertain_stage.is_dir()
          and uncertain_audit.get("complete") is False
          and len(uncertain_queue_during_recovery) == 1
          and uncertain_ledger["corrected_count"] == 0
          and uncertain_ledger["unresolved_count"] == 1
          and uncertain_ledger["queue_pending_count"] == 1,
          repr((uncertain_result, uncertain_ledger)))
    recovered_uncertain = reconcile.bank.snapshot_ratio_day(
        uncertain_root, "UNCERTAIN", "1m-iv", tue,
        today=dt.date(2024, 6, 20))
    audit.audit(uncertain_root, write_queue=True)
    check("durable recovery completes the committed correction before debt refresh",
          recovered_uncertain["stored_value"] == 0.7
          and not uncertain_stage.exists()
          and not audit.queue_snapshot(uncertain_root)
          and (((storage.load_manifest(
              uncertain_root / "UNCERTAIN").get("intervals") or {})
              .get("1m-iv") or {}).get("months") or {}).get(
                  storage.month_key(mon.year, mon.month), {}).get("sha256")
          == file_sha(uncertain_path),
          repr(recovered_uncertain))

    prep_root = fresh_root(base, "shared-preparation-cleanup-failure")
    prep_paths = write_values(
        prep_root, "PREPFAIL", "1m-iv",
        [(mon, 0.1), (tue, 0.8)], conid=91038)
    audit.audit(prep_root, write_queue=True)
    prep_row = list(reconcile.plan(prep_root))[0]
    prep_path = prep_paths[storage.month_key(mon.year, mon.month)]
    prep_manifest_path = prep_root / "PREPFAIL" / storage.MANIFEST_NAME
    prep_before = (
        prep_path.read_bytes(), prep_manifest_path.read_bytes())
    original_clear_stage = reconcile.bank._clear_stage_dir

    def fail_candidate_write(target, payload):
        target = Path(target)
        if (target.parent.name == storage.VOL_VALUE_RECONCILE_STAGE_DIR
                and storage.FILENAME_RE.fullmatch(target.name)):
            raise storage.StorageError("forced candidate write failure")
        return original_atomic_write(target, payload)

    def fail_stage_clear(_stage_dir, _candidate_name=None):
        raise reconcile.bank.RatioDayError("forced stage cleanup failure")

    storage._atomic_write_bytes = fail_candidate_write
    reconcile.bank._clear_stage_dir = fail_stage_clear
    try:
        prep_result = reconcile.reconcile_one(
            FakeAdapter([vendor_bar(tue, 0.7)], conid=91038),
            FakePacer(), prep_root, prep_row,
            run_id="shared-preparation-cleanup-failure-selftest")
    finally:
        storage._atomic_write_bytes = original_atomic_write
        reconcile.bank._clear_stage_dir = original_clear_stage
    prep_stage = (prep_root / "PREPFAIL" /
                  storage.VOL_VALUE_RECONCILE_STAGE_DIR)
    check("candidate-write cleanup failure is explicit guarded recovery debt",
          prep_result.get("status") == "ambiguous"
          and prep_result.get("commit_state") == "recovery_required"
          and prep_result.get("rollback_verified") is False
          and prep_result.get("bank_write_completed") is False
          and prep_result.get("bank_written") is False
          and prep_result.get("correction_recorded") is False
          and prep_stage.is_dir()
          and prep_before == (
              prep_path.read_bytes(), prep_manifest_path.read_bytes())
          and audit.queue_snapshot(prep_root) == [prep_row],
          repr(prep_result))
    reconcile.bank.snapshot_ratio_day(
        prep_root, "PREPFAIL", "1m-iv", tue,
        today=dt.date(2024, 6, 20))
    check("unpublished empty recovery stage is safely cleared on next snapshot",
          not prep_stage.exists())

    stage_root = fresh_root(base, "shared-staging-cleanup-failure")
    stage_paths = write_values(
        stage_root, "STAGEFAIL", "1m-iv",
        [(mon, 0.1), (tue, 0.8)], conid=91039)
    audit.audit(stage_root, write_queue=True)
    stage_row = list(reconcile.plan(stage_root))[0]
    stage_path = stage_paths[storage.month_key(mon.year, mon.month)]
    stage_manifest_path = stage_root / "STAGEFAIL" / storage.MANIFEST_NAME
    stage_before = (
        stage_path.read_bytes(), stage_manifest_path.read_bytes())
    original_save_manifest = storage.save_manifest

    def fail_staged_manifest(ticker_dir, manifest):
        if Path(ticker_dir).name == storage.VOL_VALUE_RECONCILE_STAGE_DIR:
            raise storage.StorageError("forced staged manifest failure")
        return original_save_manifest(ticker_dir, manifest)

    storage.save_manifest = fail_staged_manifest
    reconcile.bank._clear_stage_dir = fail_stage_clear
    try:
        stage_result = reconcile.reconcile_one(
            FakeAdapter([vendor_bar(tue, 0.7)], conid=91039),
            FakePacer(), stage_root, stage_row,
            run_id="shared-staging-cleanup-failure-selftest")
    finally:
        storage.save_manifest = original_save_manifest
        reconcile.bank._clear_stage_dir = original_clear_stage
    stage_marker = (stage_root / "STAGEFAIL" /
                    storage.VOL_VALUE_RECONCILE_STAGE_DIR)
    check("pre-marker staging cleanup failure is explicit guarded recovery debt",
          stage_result.get("status") == "ambiguous"
          and stage_result.get("commit_state") == "recovery_required"
          and stage_result.get("bank_write_completed") is False
          and stage_result.get("bank_written") is False
          and stage_result.get("correction_recorded") is False
          and stage_marker.is_dir()
          and stage_before == (
              stage_path.read_bytes(), stage_manifest_path.read_bytes())
          and audit.queue_snapshot(stage_root) == [stage_row],
          repr(stage_result))
    reconcile.bank.snapshot_ratio_day(
        stage_root, "STAGEFAIL", "1m-iv", tue,
        today=dt.date(2024, 6, 20))
    check("unpublished populated recovery stage is safely cleared on next snapshot",
          not stage_marker.exists())

    cleanup_root = fresh_root(base, "shared-committed-cleanup-failure")
    cleanup_paths = write_values(
        cleanup_root, "CLEANFAIL", "1m-iv",
        [(mon, 0.1), (tue, 0.8)], conid=91040)
    audit.audit(cleanup_root, write_queue=True)
    cleanup_row = list(reconcile.plan(cleanup_root))[0]
    cleanup_path = cleanup_paths[storage.month_key(mon.year, mon.month)]
    reconcile.bank._clear_stage_dir = fail_stage_clear
    try:
        cleanup_result = reconcile.reconcile_one(
            FakeAdapter([vendor_bar(tue, 0.7)], conid=91040),
            FakePacer(), cleanup_root, cleanup_row,
            run_id="shared-committed-cleanup-failure-selftest")
    finally:
        reconcile.bank._clear_stage_dir = original_clear_stage
    cleanup_stage = (cleanup_root / "CLEANFAIL" /
                     storage.VOL_VALUE_RECONCILE_STAGE_DIR)
    audit.audit(cleanup_root, write_queue=True)
    check("committed cleanup failure keeps ledger, stage fence, and queue debt",
          cleanup_result.get("status") == "ambiguous"
          and cleanup_result.get("commit_state") == "recovery_required"
          and cleanup_result.get("bank_write_completed") is True
          and cleanup_result.get("bank_written") is True
          and cleanup_result.get("correction_recorded") is True
          and cleanup_stage.is_dir()
          and audit.queue_snapshot(cleanup_root) == [cleanup_row]
          and file_sha(cleanup_path)
          == (((((storage.load_manifest(cleanup_root / "CLEANFAIL") or {})
                  .get("intervals") or {}).get("1m-iv") or {})
                .get("months") or {}).get(
                    storage.month_key(mon.year, mon.month), {})
              .get("sha256")),
          repr(cleanup_result))
    reconcile.bank.snapshot_ratio_day(
        cleanup_root, "CLEANFAIL", "1m-iv", tue,
        today=dt.date(2024, 6, 20))
    audit.audit(cleanup_root, write_queue=True)
    check("committed cleanup recovery retires only refreshed clean debt",
          not cleanup_stage.exists()
          and not audit.queue_snapshot(cleanup_root))

    csv_debt_root = fresh_root(base, "shared-csv-cleanup-debt")
    csv_debt_paths = write_values(
        csv_debt_root, "CSVDEBT", "1m-iv",
        [(mon, 0.1), (tue, 0.8)], conid=91042, fmt="csv")
    audit.audit(csv_debt_root, write_queue=True)
    csv_debt_row = list(reconcile.plan(csv_debt_root))[0]
    csv_debt_path = csv_debt_paths[storage.month_key(mon.year, mon.month)]
    original_unlink = Path.unlink
    failed_csv_unlink = False

    def fail_csv_twin_once(path, *args, **kwargs):
        nonlocal failed_csv_unlink
        if Path(path) == csv_debt_path and not failed_csv_unlink:
            failed_csv_unlink = True
            raise PermissionError("forced CSV twin cleanup failure")
        return original_unlink(path, *args, **kwargs)

    Path.unlink = fail_csv_twin_once
    try:
        csv_debt_result = reconcile.reconcile_one(
            FakeAdapter([vendor_bar(tue, 0.7)], conid=91042),
            FakePacer(), csv_debt_root, csv_debt_row,
            run_id="shared-csv-cleanup-debt-selftest")
    finally:
        Path.unlink = original_unlink
    csv_debt_stage = (csv_debt_root / "CSVDEBT" /
                      storage.VOL_VALUE_RECONCILE_STAGE_DIR)
    audit.audit(csv_debt_root, write_queue=True)
    check("CSV twin unlink failure retains committed recovery debt",
          failed_csv_unlink
          and csv_debt_result.get("status") == "ambiguous"
          and csv_debt_result.get("commit_state") == "recovery_required"
          and csv_debt_result.get("bank_written") is True
          and csv_debt_result.get("correction_recorded") is True
          and csv_debt_path.exists()
          and csv_debt_stage.is_dir()
          and audit.queue_snapshot(csv_debt_root) == [csv_debt_row],
          repr(csv_debt_result))
    reconcile.bank.snapshot_ratio_day(
        csv_debt_root, "CSVDEBT", "1m-iv", tue,
        today=dt.date(2024, 6, 20))
    audit.audit(csv_debt_root, write_queue=True)
    check("CSV twin recovery retry removes twin before clearing marker",
          not csv_debt_path.exists()
          and not csv_debt_stage.exists()
          and not audit.queue_snapshot(csv_debt_root))

    foreign_root = fresh_root(base, "shared-foreign-stage")
    write_values(
        foreign_root, "FOREIGN", "1m-iv",
        [(mon, 0.1), (tue, 0.8)], conid=91041)
    audit.audit(foreign_root, write_queue=True)
    foreign_row = list(reconcile.plan(foreign_root))[0]
    foreign_stage = (foreign_root / "FOREIGN" /
                     storage.VOL_VALUE_RECONCILE_STAGE_DIR)
    foreign_stage.mkdir()
    (foreign_stage / "foreign.txt").write_text("do not remove", encoding="utf-8")
    foreign_adapter = FakeAdapter(
        [vendor_bar(tue, 0.8)], conid=91041)
    foreign_result = reconcile.reconcile_one(
        foreign_adapter, FakePacer(), foreign_root, foreign_row,
        run_id="shared-foreign-stage-selftest")
    check("foreign recovery-stage content is refused and never removed",
          foreign_result.get("status") == "unresolved"
          and foreign_result.get("request_count") == 0
          and not foreign_adapter.fetch_calls
          and (foreign_stage / "foreign.txt").read_text(
              encoding="utf-8") == "do not remove",
          repr(foreign_result))

    append_result = {
        "blocked_months": [],
        "dup_existing": 0,
        "conflicts": 0,
        "added": 0,
        "written": 0,
        "months": {},
        "notes": [],
    }
    wed = tue + dt.timedelta(days=1)
    ibkr._commit_month(
        correct_root, "DIFF", "1m-iv", (wed.year, wed.month),
        [day_bar(wed, 0.7)], "ordinary-append-selftest", "SELFTEST",
        append_result, lambda *_args: None, conid=91002)
    appended_manifest = storage.load_manifest(correct_root / "DIFF") or {}
    appended_month = (((appended_manifest.get("intervals") or {})
                       .get("1m-iv") or {}).get("months") or {}).get(
                           storage.month_key(wed.year, wed.month), {})
    check("ordinary same-month append preserves exact-day correction ledger",
          append_result["written"] == 1
          and appended_month.get("value_corrections") == corrections
          and month_values(correct_root, "DIFF", "1m-iv")
          == {mon: 0.1, tue: 0.7, wed: 0.7},
          repr((append_result, appended_month)))

    month_name = storage.month_key(wed.year, wed.month)
    scan_same = json.loads(json.dumps(appended_manifest))
    scan_same_month = (((scan_same.get("intervals") or {})
                        .get("1m-iv") or {}).get("months") or {}).get(
                            month_name, {})
    scan_same_month.pop("value_corrections", None)
    merged_same = storage._merge_scan_manifest_for_save(
        correct_root / "DIFF", scan_same)
    merged_same_month = (((merged_same.get("intervals") or {})
                          .get("1m-iv") or {}).get("months") or {}).get(
                              month_name, {})
    check("fresh scan merge preserves concurrent ledger for exact month SHA",
          merged_same_month.get("sha256") == appended_month.get("sha256")
          and merged_same_month.get("value_corrections") == corrections,
          repr(merged_same_month))

    scan_different = json.loads(json.dumps(scan_same))
    scan_different_month = (((scan_different.get("intervals") or {})
                             .get("1m-iv") or {}).get("months") or {}).get(
                                 month_name, {})
    scan_different_month["sha256"] = "0" * 64
    merged_different = storage._merge_scan_manifest_for_save(
        correct_root / "DIFF", scan_different)
    merged_different_month = (((merged_different.get("intervals") or {})
                               .get("1m-iv") or {}).get("months") or {}).get(
                                   month_name, {})
    check("stale differing scan cannot overwrite current correction metadata",
          merged_different_month.get("sha256")
          == appended_month.get("sha256")
          and merged_different_month.get("value_corrections") == corrections,
          repr(merged_different_month))

    scanner_root = fresh_root(base, "shared-scanner-correction-race")
    scanner_paths = write_values(
        scanner_root, "SCANRACE", "1m-iv",
        [(mon, 0.1), (tue, 0.8)], conid=91044)
    audit.audit(scanner_root, write_queue=True)
    scanner_row = list(reconcile.plan(scanner_root))[0]
    scanner_path = scanner_paths[storage.month_key(mon.year, mon.month)]
    scanner_stat = scanner_path.stat()
    os.utime(scanner_path, ns=(
        scanner_stat.st_atime_ns,
        scanner_stat.st_mtime_ns + 2_000_000_000))
    scanner_captured = threading.Event()
    scanner_release = threading.Event()
    scanner_report = {}
    original_read_month = storage.read_month_file

    def blocked_scanner_read(path, *args, **kwargs):
        value = original_read_month(path, *args, **kwargs)
        if (threading.current_thread().name == "row51-stale-scanner"
                and Path(path).resolve() == scanner_path.resolve()):
            scanner_captured.set()
            if not scanner_release.wait(15):
                raise RuntimeError("scanner race fixture timed out")
        return value

    def scan_while_correction_commits():
        scanner_report["value"] = storage.scan_storage(
            scanner_root, workers=1, digest_cache=False)

    storage.read_month_file = blocked_scanner_read
    scanner_thread = threading.Thread(
        target=scan_while_correction_commits,
        name="row51-stale-scanner")
    scanner_thread.start()
    scanner_ready = scanner_captured.wait(15)
    try:
        scanner_correction = reconcile.reconcile_one(
            FakeAdapter([vendor_bar(tue, 0.7)], conid=91044),
            FakePacer(), scanner_root, scanner_row,
            run_id="shared-scanner-correction-race-selftest")
    finally:
        scanner_release.set()
        scanner_thread.join(20)
        storage.read_month_file = original_read_month
    scanner_manifest = storage.load_manifest(
        scanner_root / "SCANRACE") or {}
    scanner_entry = ((((scanner_manifest.get("intervals") or {})
                       .get("1m-iv") or {}).get("months") or {}).get(
                           storage.month_key(mon.year, mon.month), {}))
    check("stale scanner publication cannot erase a concurrent correction ledger",
          scanner_ready and not scanner_thread.is_alive()
          and scanner_correction.get("status") == "corrected"
          and scanner_entry.get("sha256") == file_sha(scanner_path)
          and len(scanner_entry.get("value_corrections") or []) == 1
          and not (scanner_report.get("value") or {}).get("warnings")
          and not audit.queue_snapshot(scanner_root),
          repr((scanner_correction, scanner_entry,
                scanner_report.get("value"))))

    same_stat = correct_path.stat()
    os.utime(correct_path, ns=(
        same_stat.st_atime_ns, same_stat.st_mtime_ns + 2_000_000_000))
    storage.scan_storage(correct_root, workers=1, digest_cache=False)
    same_scan_manifest = storage.load_manifest(correct_root / "DIFF") or {}
    same_scan_month = (((same_scan_manifest.get("intervals") or {})
                        .get("1m-iv") or {}).get("months") or {}).get(
                            month_name, {})
    check("metadata-only scan rebuild preserves exact-SHA correction ledger",
          same_scan_month.get("sha256") == file_sha(correct_path)
          and same_scan_month.get("value_corrections") == corrections,
          repr(same_scan_month))

    storage.write_month_file(correct_path, [
        day_bar(mon, 0.1), day_bar(tue, 0.71), day_bar(wed, 0.7),
    ])
    storage.scan_storage(correct_root, workers=1, digest_cache=False)
    changed_scan_manifest = storage.load_manifest(correct_root / "DIFF") or {}
    changed_scan_month = (((changed_scan_manifest.get("intervals") or {})
                           .get("1m-iv") or {}).get("months") or {}).get(
                               month_name, {})
    check("scan rebuild drops correction ledger for unproven changed bytes",
          changed_scan_month.get("sha256") == file_sha(correct_path)
          and changed_scan_month.get("sha256") != appended_month.get("sha256")
          and "value_corrections" not in changed_scan_month,
          repr(changed_scan_month))

    race_root = fresh_root(base, "shared-bank-cas-race")
    race_paths = write_values(
        race_root, "RACE", "1m-iv", [(mon, 0.1), (tue, 0.8)],
        conid=91008)
    audit.audit(race_root, write_queue=True)
    race_row = list(reconcile.plan(race_root))[0]
    race_path = race_paths[storage.month_key(mon.year, mon.month)]

    def mutate_race_month():
        storage.write_month_file(race_path, [
            day_bar(mon, 0.1),
            day_bar(tue, 0.85),
        ])

    race_adapter = FakeAdapter(
        [vendor_bar(tue, 0.7)], conid=91008,
        on_fetch=mutate_race_month)
    race_result = reconcile.reconcile_one(
        race_adapter, FakePacer(), race_root, race_row,
        run_id="shared-bank-cas-race-selftest")
    check("month change during refetch is stale and never overwritten",
          race_result.get("status") == "stale"
          and race_result.get("request_count") == 1
          and len(audit.queue_snapshot(race_root)) == 1
          and audit.queue_snapshot(race_root)[0].get("value") == 0.85
          and month_values(race_root, "RACE", "1m-iv")
          == {mon: 0.1, tue: 0.85},
          repr((race_result, month_values(
              race_root, "RACE", "1m-iv"))))

    settle_race_root = fresh_root(base, "shared-settlement-finalize-race")
    write_values(
        settle_race_root, "SETTLERACE", "1m-iv",
        [(mon, 0.1), (tue, 0.8)], conid=91042)
    audit.audit(settle_race_root, write_queue=True)
    settle_race_row = list(reconcile.plan(settle_race_root))[0]

    def mutate_before_settlement():
        write_values(
            settle_race_root, "SETTLERACE", "1m-iv",
            [(mon, 0.1), (tue, 0.85)], conid=91042)

    settle_race_result = reconcile.reconcile_one(
        FakeAdapter(
            [vendor_bar(tue, 0.8)], conid=91042,
            on_fetch=mutate_before_settlement),
        FakePacer(), settle_race_root, settle_race_row,
        run_id="shared-settlement-finalize-race-selftest")
    check("same-value settlement rechecks bank state while retiring queue debt",
          settle_race_result.get("status") == "stale"
          and settle_race_result.get("request_count") == 1
          and len(audit.queue_snapshot(settle_race_root)) == 1
          and audit.queue_snapshot(settle_race_root)[0].get("value") == 0.85
          and not (settle_race_root / audit.SETTLED_BASENAME).exists()
          and month_values(
              settle_race_root, "SETTLERACE", "1m-iv")[tue] == 0.85,
          repr(settle_race_result))

    final_race_root = fresh_root(base, "shared-correction-finalize-race")
    write_values(
        final_race_root, "FINALRACE", "1m-iv",
        [(mon, 0.1), (tue, 0.8)], conid=91043)
    audit.audit(final_race_root, write_queue=True)
    final_race_row = list(reconcile.plan(final_race_root))[0]
    original_finalization_guard = reconcile.bank.finalization_guard
    final_append = {
        "blocked_months": [], "dup_existing": 0, "conflicts": 0,
        "added": 0, "written": 0, "months": {}, "notes": [],
    }

    @contextmanager
    def append_before_finalization(
            root, snapshot, served, *, action, correction=None,
            manifest_lock=None):
        if action == "corrected":
            ibkr._commit_month(
                root, "FINALRACE", "1m-iv", (tue.year, tue.month),
                [day_bar(tue, 0.65, minute=9 * 60 + 30)],
                "shared-correction-finalize-race-writer", "SELFTEST",
                final_append, lambda *_args: None, conid=91043)
        with original_finalization_guard(
                root, snapshot, served, action=action,
                correction=correction, manifest_lock=manifest_lock):
            yield

    reconcile.bank.finalization_guard = append_before_finalization
    try:
        final_race_result = reconcile.reconcile_one(
            FakeAdapter([vendor_bar(tue, 0.7)], conid=91043),
            FakePacer(), final_race_root, final_race_row,
            run_id="shared-correction-finalize-race-selftest")
    finally:
        reconcile.bank.finalization_guard = original_finalization_guard
    final_race_manifest = storage.load_manifest(
        final_race_root / "FINALRACE") or {}
    final_race_entry = ((((final_race_manifest.get("intervals") or {})
                          .get("1m-iv") or {}).get("months") or {}).get(
                              storage.month_key(mon.year, mon.month), {}))
    final_race_ledger = reconcile.ledger([final_race_result])
    check("post-commit finalization drift preserves durable correction truth",
          final_race_result.get("status") == "corrected"
          and final_race_result.get("bank_committed") is True
          and final_race_result.get("queue_resolved") is False
          and final_race_result.get("finalization_verified") is False
          and final_race_result.get("request_count") == 1
          and final_append["written"] == 1
          and audit.queue_snapshot(final_race_root) == [final_race_row]
          and len(final_race_entry.get("value_corrections") or []) == 1
          and final_race_entry.get("sha256")
          == file_sha(storage.find_month_file(
              final_race_root, "FINALRACE", mon.year, mon.month,
              "1m-iv"))
          and final_race_ledger["corrected_count"] == 1
          and final_race_ledger["queue_pending_count"] == 1,
          repr((final_race_result, final_append, final_race_entry,
                final_race_ledger)))

    empty_root = fresh_root(base, "shared-no-data")
    empty_paths = write_values(
        empty_root, "EMPTY", "1m-iv", [(mon, 0.1), (tue, 0.8)],
        conid=91003)
    audit.audit(empty_root, write_queue=True)
    empty_row = list(reconcile.plan(empty_root))[0]
    empty_path = empty_paths[storage.month_key(mon.year, mon.month)]
    empty_before = (
        file_sha(empty_path),
        (empty_root / "EMPTY" / storage.MANIFEST_NAME).read_bytes(),
    )
    empty_adapter = FakeAdapter([], conid=91003)
    unresolved = reconcile.reconcile_one(
        empty_adapter, FakePacer(), empty_root, empty_row,
        run_id="shared-empty-selftest")
    empty_after = (
        file_sha(empty_path),
        (empty_root / "EMPTY" / storage.MANIFEST_NAME).read_bytes(),
    )
    check("no-data replay remains unresolved with bank, manifest, and queue unchanged",
          unresolved.get("status") == "unresolved"
          and result_key(unresolved) == queue_key(empty_row)
          and unresolved.get("request_count") == 1
          and len(empty_adapter.fetch_calls) == 1
          and empty_after == empty_before
          and audit.queue_snapshot(empty_root) == [empty_row],
          repr(unresolved))

    source_error_root = fresh_root(base, "shared-source-exception")
    source_error_paths = write_values(
        source_error_root, "SOURCE", "1m-iv",
        [(mon, 0.1), (tue, 0.8)], conid=91009)
    audit.audit(source_error_root, write_queue=True)
    source_error_row = list(reconcile.plan(source_error_root))[0]
    source_error_path = source_error_paths[
        storage.month_key(mon.year, mon.month)]
    source_error_before = (
        file_sha(source_error_path),
        (source_error_root / "SOURCE" / storage.MANIFEST_NAME).read_bytes(),
    )
    source_error_adapter = FakeAdapter(
        [], conid=91009, fetch_error=ConnectionError("fixture source down"))
    source_error_result = reconcile.reconcile_one(
        source_error_adapter, FakePacer(), source_error_root,
        source_error_row, run_id="shared-source-exception-selftest")
    source_error_after = (
        file_sha(source_error_path),
        (source_error_root / "SOURCE" / storage.MANIFEST_NAME).read_bytes(),
    )
    check("source exception is unresolved but truthfully counts its request",
          source_error_result.get("status") == "unresolved"
          and source_error_result.get("request_count") == 1
          and len(source_error_adapter.fetch_calls) == 1
          and source_error_after == source_error_before
          and audit.queue_snapshot(source_error_root) == [source_error_row],
          repr(source_error_result))

    invalid_root = fresh_root(base, "shared-invalid-source-bar")
    invalid_paths = write_values(
        invalid_root, "INVALID", "1m-iv",
        [(mon, 0.1), (tue, 0.8)], conid=91010)
    audit.audit(invalid_root, write_queue=True)
    invalid_row = list(reconcile.plan(invalid_root))[0]
    invalid_path = invalid_paths[storage.month_key(mon.year, mon.month)]
    invalid_before = (
        file_sha(invalid_path),
        (invalid_root / "INVALID" / storage.MANIFEST_NAME).read_bytes(),
    )
    invalid_raw = vendor_bar(tue, 0.8)
    invalid_raw.high = 11.0
    invalid_adapter = FakeAdapter([invalid_raw], conid=91010)
    invalid_result = reconcile.reconcile_one(
        invalid_adapter, FakePacer(), invalid_root, invalid_row,
        run_id="shared-invalid-source-selftest")
    invalid_after = (
        file_sha(invalid_path),
        (invalid_root / "INVALID" / storage.MANIFEST_NAME).read_bytes(),
    )
    check("over-ceiling source bar is unresolved and counts one request",
          invalid_result.get("status") == "unresolved"
          and invalid_result.get("request_count") == 1
          and len(invalid_adapter.fetch_calls) == 1
          and invalid_after == invalid_before
          and audit.queue_snapshot(invalid_root) == [invalid_row],
          repr(invalid_result))

    endless_root = fresh_root(base, "shared-endless-source")
    endless_paths = write_values(
        endless_root, "ENDLESS", "1m-iv",
        [(mon, 0.1), (tue, 0.8)], conid=91036)
    audit.audit(endless_root, write_queue=True)
    endless_row = list(reconcile.plan(endless_root))[0]
    endless_path = endless_paths[storage.month_key(mon.year, mon.month)]
    endless_before = (
        endless_path.read_bytes(),
        (endless_root / "ENDLESS" / storage.MANIFEST_NAME).read_bytes(),
    )
    endless_adapter = EndlessAdapter(tue, conid=91036)
    endless_result = reconcile.reconcile_one(
        endless_adapter, FakePacer(), endless_root, endless_row,
        run_id="shared-endless-source-selftest")
    check("faulty endless source is cut off at the physical one-day bound",
          endless_result.get("status") == "unresolved"
          and endless_result.get("request_count") == 1
          and endless_adapter.produced
          == reconcile.bank._raw_response_limit("1m-iv") + 1
          and endless_adapter.produced < 500
          and endless_before == (
              endless_path.read_bytes(),
              (endless_root / "ENDLESS" /
               storage.MANIFEST_NAME).read_bytes())
          and audit.queue_snapshot(endless_root) == [endless_row],
          repr((endless_result, endless_adapter.produced)))

    neighbor_root = fresh_root(base, "shared-invalid-neighbor-day")
    neighbor_paths = write_values(
        neighbor_root, "NEIGHBOR", "1m-iv",
        [(mon, 25.0), (tue, 0.8)], conid=91011)
    audit.audit(neighbor_root, write_queue=True)
    neighbor_row = next(
        row for row in reconcile.plan(neighbor_root)
        if row["day"] == tue.isoformat())
    neighbor_path = neighbor_paths[storage.month_key(mon.year, mon.month)]
    neighbor_adapter = FakeAdapter([vendor_bar(tue, 0.7)], conid=91011)
    neighbor_result = reconcile.reconcile_one(
        neighbor_adapter, FakePacer(), neighbor_root, neighbor_row,
        run_id="shared-invalid-neighbor-selftest")
    neighbor_pending = list(reconcile.plan(neighbor_root))
    neighbor_second = reconcile.reconcile_one(
        FakeAdapter([vendor_bar(mon, 0.1)], conid=91011),
        FakePacer(), neighbor_root, neighbor_pending[0],
        run_id="shared-invalid-neighbor-second-selftest")
    neighbor_after_second = list(reconcile.plan(neighbor_root))
    neighbor_cleanup_adapter = FakeAdapter(
        [vendor_bar(tue, 0.7)], conid=91011)
    neighbor_cleanup = reconcile.reconcile_one(
        neighbor_cleanup_adapter, FakePacer(), neighbor_root,
        neighbor_after_second[0],
        run_id="shared-invalid-neighbor-context-refresh-selftest")
    neighbor_manifest = storage.load_manifest(
        neighbor_root / "NEIGHBOR") or {}
    neighbor_entry = ((((neighbor_manifest.get("intervals") or {})
                        .get("1m-iv") or {}).get("months") or {}).get(
                            storage.month_key(mon.year, mon.month), {}))
    check("two invalid days in one month can be repaired sequentially",
          neighbor_result.get("status") == "corrected"
          and neighbor_result.get("request_count") == 1
          and len(neighbor_pending) == 1
          and neighbor_pending[0]["day"] == mon.isoformat()
          and neighbor_second.get("status") == "corrected"
          and neighbor_second.get("request_count") == 1
          and len(neighbor_after_second) == 1
          and neighbor_after_second[0]["day"] == tue.isoformat()
          and neighbor_cleanup.get("status") == "stale"
          and neighbor_cleanup.get("request_count") == 0
          and not neighbor_cleanup_adapter.fetch_calls
          and month_values(neighbor_root, "NEIGHBOR", "1m-iv")
          == {mon: 0.1, tue: 0.7}
          and len(neighbor_entry.get("value_corrections") or []) == 2
          and not audit.queue_snapshot(neighbor_root),
          repr((neighbor_result, neighbor_second, neighbor_after_second,
                neighbor_cleanup, neighbor_entry)))

    stale_root = fresh_root(base, "shared-stale-bank-day")
    write_values(
        stale_root, "STALE", "1m-iv", [(mon, 0.1), (tue, 0.8)],
        conid=91007)
    audit.audit(stale_root, write_queue=True)
    stale_row = list(reconcile.plan(stale_root))[0]
    write_values(
        stale_root, "STALE", "1m-iv", [(mon, 0.1), (tue, 0.9)],
        conid=91007)
    stale_adapter = FakeAdapter([vendor_bar(tue, 0.9)], conid=91007)
    stale_result = reconcile.reconcile_one(
        stale_adapter, FakePacer(), stale_root, stale_row,
        run_id="shared-stale-bank-selftest")
    check("queue/bank value drift is stale before spending a source request",
          stale_result.get("status") == "stale"
          and stale_result.get("request_count") == 0
          and not stale_adapter.fetch_calls
          and len(audit.queue_snapshot(stale_root)) == 1
          and audit.queue_snapshot(stale_root)[0].get("value") == 0.9,
          repr(stale_result))

    floor_root = fresh_root(base, "shared-identity-floor")
    write_values(
        floor_root, "FLOOR", "1m-iv", [(mon, 0.1), (tue, 0.8)],
        conid=91004)
    audit.audit(floor_root, write_queue=True)
    floor_row = list(reconcile.plan(floor_root))[0]
    floor_manifest = storage.load_manifest(floor_root / "FLOOR")
    floor_manifest["data_corrections"] = [{
        "type": "identity_listing_truncation",
        "ticker": "FLOOR",
        "cutover": "2024-06-19",
    }]
    storage.save_manifest(floor_root / "FLOOR", floor_manifest)
    floor_adapter = FakeAdapter([vendor_bar(tue, 0.8)], conid=91004)
    floor_result = reconcile.reconcile_one(
        floor_adapter, FakePacer(), floor_root, floor_row,
        run_id="shared-identity-floor-selftest")
    check("identity floor rejects predecessor days before spending a request",
          floor_result.get("status") == "unresolved"
          and floor_result.get("request_count") == 0
          and not floor_adapter.fetch_calls
          and audit.queue_snapshot(floor_root) == [floor_row],
          repr(floor_result))

    floor_race_root = fresh_root(base, "shared-identity-floor-cas-race")
    floor_race_paths = write_values(
        floor_race_root, "FLOORRACE", "1m-iv",
        [(mon, 0.1), (tue, 0.8)], conid=91046)
    audit.audit(floor_race_root, write_queue=True)
    floor_race_row = list(reconcile.plan(floor_race_root))[0]
    floor_race_path = floor_race_paths[
        storage.month_key(mon.year, mon.month)]
    floor_race_month_before = floor_race_path.read_bytes()

    def change_floor_without_excluding_day():
        manifest = storage.load_manifest(floor_race_root / "FLOORRACE")
        manifest["data_corrections"] = [{
            "type": "identity_listing_truncation",
            "ticker": "FLOORRACE",
            "cutover": mon.isoformat(),
        }]
        storage.save_manifest(floor_race_root / "FLOORRACE", manifest)

    floor_race_adapter = FakeAdapter(
        [vendor_bar(tue, 0.7)], conid=91046,
        on_fetch=change_floor_without_excluding_day)
    floor_race_result = reconcile.reconcile_one(
        floor_race_adapter, FakePacer(), floor_race_root, floor_race_row,
        run_id="shared-identity-floor-cas-race-selftest")
    floor_race_manifest = storage.load_manifest(
        floor_race_root / "FLOORRACE") or {}
    floor_race_entry = ((((floor_race_manifest.get("intervals") or {})
                          .get("1m-iv") or {}).get("months") or {}).get(
                              storage.month_key(mon.year, mon.month), {}))
    check("identity-floor drift exact-CASes before staging a correction",
          floor_race_result.get("status") == "stale"
          and floor_race_result.get("request_count") == 1
          and len(floor_race_adapter.fetch_calls) == 1
          and floor_race_path.read_bytes() == floor_race_month_before
          and not floor_race_entry.get("value_corrections")
          and not (floor_race_root / "FLOORRACE" /
                   storage.VOL_VALUE_RECONCILE_STAGE_DIR).exists()
          and audit.queue_snapshot(floor_race_root) == [floor_race_row],
          repr((floor_race_result, floor_race_entry)))

    earliest_root = fresh_root(base, "shared-kind-earliest")
    write_values(
        earliest_root, "EARLIEST", "1m-iv",
        [(mon, 0.1), (tue, 0.8)], conid=91037)
    audit.audit(earliest_root, write_queue=True)
    earliest_row = list(reconcile.plan(earliest_root))[0]
    earliest_manifest = storage.load_manifest(
        earliest_root / "EARLIEST") or {}
    earliest_section = ((earliest_manifest.get("intervals") or {})
                        .get("1m-iv") or {})
    earliest_section["backfill_served_earliest"] = "2024-06-19"
    storage.save_manifest(earliest_root / "EARLIEST", earliest_manifest)
    earliest_adapter = FakeAdapter(
        [vendor_bar(tue, 0.8)], conid=91037)
    earliest_result = reconcile.reconcile_one(
        earliest_adapter, FakePacer(), earliest_root, earliest_row,
        run_id="shared-kind-earliest-selftest")
    check("exact per-kind source reach rejects an earlier day before a request",
          earliest_result.get("status") == "unresolved"
          and earliest_result.get("request_count") == 0
          and not earliest_adapter.fetch_calls
          and audit.queue_snapshot(earliest_root) == [earliest_row],
          repr(earliest_result))

    sub_root = fresh_root(base, "shared-multi-request-refusal")
    write_values(
        sub_root, "SUB", "1s-iv", [(mon, 0.1), (tue, 0.8)],
        conid=91005)
    audit.audit(sub_root, write_queue=True)
    sub_row = list(reconcile.plan(sub_root))[0]
    sub_adapter = FakeAdapter([vendor_bar(tue, 0.8)], conid=91005)
    sub_result = reconcile.reconcile_one(
        sub_adapter, FakePacer(), sub_root, sub_row,
        run_id="shared-multi-request-selftest")
    check("multi-request subminute day fails closed before any historical call",
          sub_result.get("status") == "unresolved"
          and sub_result.get("request_count") == 0
          and not sub_adapter.fetch_calls
          and audit.queue_snapshot(sub_root) == [sub_row],
          repr(sub_result))

    evidence_rows = [
        same, hvol_result, corrected, race_result, unresolved,
        stale_result, floor_result, sub_result,
    ]
    summary = reconcile.ledger(
        evidence_rows, requested_count=len(evidence_rows))
    check("shared ledger reports exact outcome and request counters",
          summary["kind"] == "vol_value_reconcile"
          and summary["requested_count"] == summary["completed_count"] == 8
          and summary["settled_count"] == 2
          and summary["corrected_count"] == 1
          and summary["unresolved_count"] == 5
          and summary["queue_resolved_count"] == 3
          and summary["request_count"] == 5,
          repr(summary))
    oversized = reconcile.ledger(
        [same] * (reconcile.MAX_LEDGER_ROWS + 2))
    check("shared ledger bounds embedded result rows without hiding totals",
          len(oversized["rows"]) == reconcile.MAX_LEDGER_ROWS
          and oversized["completed_count"] == reconcile.MAX_LEDGER_ROWS + 2
          and oversized["rows_truncated"] == 2,
          repr({key: oversized[key] for key in (
              "completed_count", "rows_truncated")}))

    partial = reconcile.ledger([same], requested_count=3)
    contradictory = dict(same)
    contradictory["request_count_unknown"] = True
    check("shared ledger counts unstarted selected rows as durable queue debt",
          partial["completed_count"] == 1
          and partial["requested_count"] == 3
          and partial["queue_resolved_count"] == 1
          and partial["queue_pending_count"] == 2,
          repr(partial))
    check("shared ledger rejects contradictory known/unknown request evidence",
          rejects(lambda: reconcile.ledger([contradictory])))
    false_retirement = {
        "status": "unresolved", "ticker": "HOSTILE",
        "kind": "1m-iv", "kind_token": "1m-iv",
        "day": tue.isoformat(), "request_count": 0,
        "queue_resolved": True,
    }
    unproved_settlement = dict(
        same, registry_committed=False, queue_resolved=True)
    check("shared ledger rejects false queue retirement and missing action proof",
          rejects(lambda: reconcile.ledger([false_retirement]))
          and rejects(lambda: reconcile.ledger([unproved_settlement])))
    original_item_limit = reconcile.MAX_LEDGER_ITEMS
    reconcile.MAX_LEDGER_ITEMS = 3
    try:
        over_limit_rejected = rejects(lambda: reconcile.ledger(
            (same for _index in range(4))))
    finally:
        reconcile.MAX_LEDGER_ITEMS = original_item_limit
    check("shared ledger streams and refuses an over-limit row iterable",
          over_limit_rejected)

    # Exercise the production-shaped Fix Data composition while the parent
    # already owns the same fetch gate. Every bank byte remains under this
    # suite's TemporaryDirectory and the only source is FakeAdapter.
    pipeline = importlib.import_module("fix_data_pipeline")
    operation_gate = importlib.import_module("operation_gate")
    composed_root = fresh_root(base, "shared-fixdata-composition")
    write_values(
        composed_root, "PIPE", "1m-iv", [(mon, 0.1), (tue, 0.8)],
        conid=91020)
    audit.audit(composed_root, write_queue=True)
    run_logs = composed_root.parent / "Run Logs"
    artifact = run_logs / "fix-data-vol-value-reconcile-composed.json"
    adapters = []

    def composed_adapter(_port):
        value = FakeAdapter([vendor_bar(tue, 0.8)], conid=91020)
        adapters.append(value)
        return value

    def composed_plan(root, *, ticker, kinds=None):
        # This must re-enter the parent's fetch mode, not ask for the mutually
        # exclusive standalone audit mode.  The initial unscoped call selects
        # candidate tickers only; the second call is a fresh post-fill exact
        # ticker re-audit and supplies the executable rows.
        scope = None if ticker is None else [ticker]
        report = audit.audit(
            root, tickers=scope, write_queue=True, operation_mode="fetch")
        if report.get("complete") is not True:
            raise RuntimeError("composed volatility audit is incomplete")
        return reconcile.plan(
            root, tickers=scope, kinds=["1m-iv"])

    def composed_report(root, run_id, rows, *, requested_count, summary):
        return reconcile.write_artifact(
            artifact, rows, bank_root=root, run_logs_root=run_logs,
            run_id=run_id, requested_count=requested_count, summary=summary)

    old_lock = operation_gate.LOCK_PATH
    operation_gate.LOCK_PATH = run_logs / ".market_data_operation.lock"
    try:
        composed = pipeline.run(
            root=composed_root, ports=(2000,),
            adapter_factory=composed_adapter,
            scan_fn=lambda *_args, **_kwargs: {
                "summary": {}, "verification_series": []},
            health_fn=lambda _root: {},
            fill_fn=lambda *_args, **_kwargs: {},
            audit_fn=lambda *_args, **_kwargs: {"status": "ok"},
            series_fn=lambda _root: [],
            refresh_fn=lambda *_args, **_kwargs: {},
            run_id="shared-fixdata-composition",
            reconcile_plan_fn=composed_plan,
            reconcile_fn=reconcile.reconcile_one,
            reconcile_pacer_factory=FakePacer,
            reconcile_report_fn=composed_report,
            kinds=("iv",),
        )
    finally:
        operation_gate.LOCK_PATH = old_lock
    artifact_body = json.loads(artifact.read_text(encoding="utf-8"))
    check("real Fix Data composition re-enters fetch audit and settles once",
          composed["error"] is None
          and composed["reconcile_selected"] == 1
          and composed["reconcile_completed"] == 1
          and composed["reconcile_request_count"] == 1
          and composed["reconcile_request_unknown"] == 0
          and composed["reconcile_settled"] == 1
          and composed["reconcile_queue_pending"] == 0
          and len(adapters) == 1
          and len(adapters[0].fetch_calls) == 1
          and adapters[0].disconnect_calls == 1
          and reconcile.plan(composed_root) == [],
          repr(composed))
    check("Fix Data writes a bounded truthful artifact outside the bank",
          composed["reconcile_report"] == str(artifact.resolve())
          and artifact_body["parent_run_id"]
          == "shared-fixdata-composition"
          and artifact_body["artifact_written"] is True
          and artifact_body["market_data_written"] is False
          and artifact_body["bank_metadata_written"] is True
          and artifact_body["bank_written"] is True
          and artifact_body["written"] is True
          and artifact_body["settled_count"] == 1
          and artifact_body["queue_pending_count"] == 0
          and rejects(lambda: reconcile.write_artifact(
              run_logs / "nested" / "bad.json", [],
              bank_root=composed_root, run_logs_root=run_logs,
              run_id="bad-path")),
          repr(artifact_body))

    oversized_artifact = run_logs / "oversized-reconcile.json"
    nan_artifact = run_logs / "nan-reconcile.json"
    oversized_row = dict(same, error="x" * reconcile.MAX_ARTIFACT_BYTES)
    nan_row = dict(same, served_value=float("nan"))
    check("artifact size rejection occurs before any target is published",
          rejects(lambda: reconcile.write_artifact(
              oversized_artifact, [oversized_row], bank_root=composed_root,
              run_logs_root=run_logs, run_id="oversized-artifact"))
          and not oversized_artifact.exists())
    check("non-JSON numeric evidence is rejected without a partial target",
          rejects(lambda: reconcile.write_artifact(
              nan_artifact, [nan_row], bank_root=composed_root,
              run_logs_root=run_logs, run_id="nan-artifact"))
          and not nan_artifact.exists())


def run_crash_worker(root: Path, phase: str) -> int:
    """Child-only hard-exit injector; every fixture is supplied by the parent."""
    reconcile = importlib.import_module("vol_value_reconcile")
    rows = list(reconcile.plan(root))
    if len(rows) != 1:
        return 71
    row = rows[0]
    manifest = storage.load_manifest(root / row["ticker"]) or {}
    conid = int(manifest.get("conid") or 0)
    day = dt.date.fromisoformat(row["day"])
    ticker_dir = root / row["ticker"]
    manifest_path = ticker_dir / storage.MANIFEST_NAME

    if phase in {"candidate_temp", "backup_temp"}:
        original_replace = os.replace

        def exit_with_backup_temp(source, target):
            target = Path(target)
            if (phase == "candidate_temp"
                    and target.parent.name
                    == storage.VOL_VALUE_RECONCILE_STAGE_DIR
                    and storage.FILENAME_RE.fullmatch(target.name)):
                os._exit(89)
            if (target.parent.name == storage.VOL_VALUE_RECONCILE_STAGE_DIR
                    and target.name == reconcile.bank.TXN_MONTH_BEFORE):
                os._exit(90)
            return original_replace(source, target)

        os.replace = exit_with_backup_temp
    elif phase == "marker":
        original_atomic = storage._atomic_write_bytes

        def exit_after_marker(target, payload):
            if Path(target) == manifest_path:
                os._exit(91)
            return original_atomic(target, payload)

        storage._atomic_write_bytes = exit_after_marker
    elif phase == "manifest":
        reconcile.bank._publish_prepared_month = (
            lambda *_args, **_kwargs: os._exit(92))
    elif phase in {"month", "month_csv"}:
        original_publish = reconcile.bank._publish_prepared_month

        def exit_after_month(*args, **kwargs):
            original_publish(*args, **kwargs)
            os._exit(93)

        reconcile.bank._publish_prepared_month = exit_after_month
    elif phase == "csv_cleanup":
        original_unlink = Path.unlink

        def exit_after_csv_cleanup(path, *args, **kwargs):
            if (path.suffix.casefold() == ".csv"
                    and path.parent.name in storage.MONTH_DIRS):
                result = original_unlink(path, *args, **kwargs)
                os._exit(96)
                return result
            return original_unlink(path, *args, **kwargs)

        Path.unlink = exit_after_csv_cleanup
    elif phase in {"cleanup_marker", "cleanup_backup"}:
        original_unlink = Path.unlink
        marker_deleted = False

        def exit_during_cleanup(path, *args, **kwargs):
            nonlocal marker_deleted
            in_stage = (
                path.parent.name == storage.VOL_VALUE_RECONCILE_STAGE_DIR)
            if in_stage and path.name == reconcile.bank.TXN_MARKER_NAME:
                result = original_unlink(path, *args, **kwargs)
                marker_deleted = True
                if phase == "cleanup_marker":
                    os._exit(94)
                return result
            if in_stage and marker_deleted and phase == "cleanup_backup":
                result = original_unlink(path, *args, **kwargs)
                os._exit(95)
                return result
            return original_unlink(path, *args, **kwargs)

        Path.unlink = exit_during_cleanup
    else:
        return 72

    reconcile.reconcile_one(
        FakeAdapter([vendor_bar(day, 0.7)], conid=conid),
        FakePacer(), root, row,
        run_id=f"hard-exit-{phase}-selftest")
    return 73


def run_hard_exit_recovery_checks(base: Path) -> None:
    print("=== process-death transaction recovery ===========================")
    reconcile = importlib.import_module("vol_value_reconcile")
    mon = dt.date(2024, 6, 17)
    tue = mon + dt.timedelta(days=1)
    phases = [
        ("candidate_temp", False, "parquet", 89),
        ("backup_temp", False, "parquet", 90),
        ("marker", False, "parquet", 91),
        ("manifest", False, "parquet", 92),
        ("month", True, "parquet", 93),
        ("month_csv", True, "csv", 93),
        ("csv_cleanup", True, "csv", 96),
        ("cleanup_marker", True, "parquet", 94),
        ("cleanup_backup", True, "parquet", 95),
    ]
    for index, (phase, committed, fmt, exit_code) in enumerate(phases):
        root = fresh_root(base, f"hard-exit-{phase}")
        ticker = f"CRASH{index}"
        write_values(
            root, ticker, "1m-iv", [(mon, 0.1), (tue, 0.8)],
            conid=92000 + index, fmt=fmt)
        audit.audit(root, write_queue=True)
        row = list(reconcile.plan(root))[0]
        completed = subprocess.run(
            [sys.executable, "-B", str(Path(__file__).resolve()),
             "--crash-worker", str(root), phase],
            cwd=str(Path(__file__).resolve().parent.parent),
            capture_output=True, text=True, timeout=30, check=False)
        stage = root / ticker / storage.VOL_VALUE_RECONCILE_STAGE_DIR
        audit.audit(root, write_queue=True)
        fenced_queue = list(audit.queue_snapshot(root))
        fenced = stage.is_dir() and fenced_queue == [row]
        recovered = reconcile.bank.snapshot_ratio_day(
            root, ticker, "1m-iv", tue,
            today=dt.date(2024, 6, 20))
        active = storage.find_month_file(
            root, ticker, tue.year, tue.month, "1m-iv")
        manifest = storage.load_manifest(root / ticker) or {}
        entry = ((((manifest.get("intervals") or {}).get("1m-iv") or {})
                  .get("months") or {}).get(
                      storage.month_key(tue.year, tue.month), {}))
        coherent = (
            active is not None and entry.get("sha256") == file_sha(active)
            and not stage.exists())
        audit.audit(root, write_queue=True)
        final_queue = list(audit.queue_snapshot(root))
        ledger = entry.get("value_corrections") or []
        expected_state = (
            recovered["stored_value"] == (0.7 if committed else 0.8)
            and ((not final_queue and bool(ledger)) if committed
                 else (len(final_queue) == 1 and not ledger)))
        csv_clean = True
        if phase in {"month_csv", "csv_cleanup"}:
            csv_clean = not storage.month_file_path(
                root, ticker, tue.year, tue.month, "1m-iv",
                fmt="csv").exists()
        check(
            f"hard exit {phase} recovers to exact "
            f"{'new' if committed else 'old'} state",
            completed.returncode == exit_code
            and fenced and coherent and expected_state and csv_clean,
            repr({
                "returncode": completed.returncode,
                "stderr": completed.stderr[-500:],
                "fenced": fenced, "coherent": coherent,
                "stored_value": recovered.get("stored_value"),
                "final_queue": final_queue, "ledger": ledger,
                "csv_clean": csv_clean,
            }))

    public_root = fresh_root(base, "hard-exit-public-retry")
    write_values(
        public_root, "PUBLIC", "1m-iv", [(mon, 0.1), (tue, 0.8)],
        conid=92100)
    audit.audit(public_root, write_queue=True)
    public_row = list(reconcile.plan(public_root))[0]
    public_exit = subprocess.run(
        [sys.executable, "-B", str(Path(__file__).resolve()),
         "--crash-worker", str(public_root), "month"],
        cwd=str(Path(__file__).resolve().parent.parent),
        capture_output=True, text=True, timeout=30, check=False)
    public_adapter = FakeAdapter(
        [vendor_bar(tue, 0.7)], conid=92100)
    public_retry = reconcile.reconcile_one(
        public_adapter, FakePacer(), public_root, public_row,
        run_id="hard-exit-public-retry-selftest")
    public_stage = (public_root / "PUBLIC" /
                    storage.VOL_VALUE_RECONCILE_STAGE_DIR)
    public_manifest = storage.load_manifest(public_root / "PUBLIC") or {}
    public_entry = ((((public_manifest.get("intervals") or {})
                      .get("1m-iv") or {}).get("months") or {}).get(
                          storage.month_key(mon.year, mon.month), {}))
    check("public reconcile retry recovers WAL before narrowing its queue",
          public_exit.returncode == 93
          and public_retry.get("status") == "stale"
          and public_retry.get("request_count") == 0
          and not public_adapter.fetch_calls
          and not public_stage.exists()
          and not audit.queue_snapshot(public_root)
          and bool(public_entry.get("value_corrections")),
          repr((public_exit.returncode, public_exit.stderr[-300:],
                public_retry, public_entry)))

    path_root = fresh_root(base, "hard-exit-tampered-path")
    write_values(
        path_root, "PATHBAD", "1m-iv", [(mon, 0.1), (tue, 0.8)],
        conid=92101)
    audit.audit(path_root, write_queue=True)
    path_row = list(reconcile.plan(path_root))[0]
    path_exit = subprocess.run(
        [sys.executable, "-B", str(Path(__file__).resolve()),
         "--crash-worker", str(path_root), "month"],
        cwd=str(Path(__file__).resolve().parent.parent),
        capture_output=True, text=True, timeout=30, check=False)
    path_stage = (path_root / "PATHBAD" /
                  storage.VOL_VALUE_RECONCILE_STAGE_DIR)
    marker_path = path_stage / reconcile.bank.TXN_MARKER_NAME
    marker_before = marker_path.read_bytes()
    marker = json.loads(marker_before.decode("utf-8"))
    foreign_dir = path_root / "PATHBAD" / "other"
    foreign_dir.mkdir()
    foreign_path = foreign_dir / marker["candidate_name"]
    foreign_payload = b"do-not-delete-unproven-in-ticker-path"
    foreign_path.write_bytes(foreign_payload)
    marker["write_rel"] = foreign_path.relative_to(
        path_root / "PATHBAD").as_posix()
    storage._atomic_write_bytes(
        marker_path, json.dumps(
            marker, sort_keys=True, separators=(",", ":")).encode("utf-8"))
    path_queue_before = queue_bytes(path_root)
    path_adapter = FakeAdapter(
        [vendor_bar(tue, 0.7)], conid=92101)
    path_refused = reconcile.reconcile_one(
        path_adapter, FakePacer(), path_root, path_row,
        run_id="hard-exit-tampered-path-refused-selftest")
    path_refused_state = (
        path_stage.is_dir(), marker_path.read_bytes(),
        foreign_path.read_bytes(), queue_bytes(path_root))
    storage._atomic_write_bytes(marker_path, marker_before)
    path_recovery_adapter = FakeAdapter(
        [vendor_bar(tue, 0.7)], conid=92101)
    path_recovered = reconcile.reconcile_one(
        path_recovery_adapter, FakePacer(), path_root, path_row,
        run_id="hard-exit-tampered-path-restored-selftest")
    check("tampered in-ticker WAL path is inert until exact marker restore",
          path_exit.returncode == 93
          and path_refused.get("status") == "unresolved"
          and path_refused.get("request_count") == 0
          and not path_adapter.fetch_calls
          and path_refused_state[0]
          and path_refused_state[1] != marker_before
          and path_refused_state[2] == foreign_payload
          and path_refused_state[3] == path_queue_before
          and path_recovered.get("status") == "stale"
          and path_recovered.get("request_count") == 0
          and not path_recovery_adapter.fetch_calls
          and not path_stage.exists()
          and foreign_path.read_bytes() == foreign_payload
          and not audit.queue_snapshot(path_root),
          repr((path_exit.returncode, path_refused, path_recovered)))

    csv_root = fresh_root(base, "changed-legacy-csv-pending")
    csv_paths = write_values(
        csv_root, "CSVCHG", "1m-iv", [(mon, 0.1), (tue, 0.8)],
        conid=92102, fmt="csv")
    audit.audit(csv_root, write_queue=True)
    csv_row = list(reconcile.plan(csv_root))[0]
    csv_path = csv_paths[storage.month_key(mon.year, mon.month)]
    csv_before = csv_path.read_bytes()
    csv_changed_stats = storage.write_month_file(csv_path, [
        day_bar(mon, 0.1), day_bar(tue, 0.66),
    ])
    csv_changed = csv_path.read_bytes()
    storage._atomic_write_bytes(csv_path, csv_before)
    original_publish = reconcile.bank._publish_prepared_month

    def publish_then_change_csv(staged_path, write_path, expected_sha,
                                expected_before_sha):
        original_publish(
            staged_path, write_path, expected_sha, expected_before_sha)
        storage._atomic_write_bytes(csv_path, csv_changed)

    reconcile.bank._publish_prepared_month = publish_then_change_csv
    try:
        csv_pending = reconcile.reconcile_one(
            FakeAdapter([vendor_bar(tue, 0.7)], conid=92102),
            FakePacer(), csv_root, csv_row,
            run_id="changed-legacy-csv-pending-selftest")
    finally:
        reconcile.bank._publish_prepared_month = original_publish
    csv_stage = (csv_root / "CSVCHG" /
                 storage.VOL_VALUE_RECONCILE_STAGE_DIR)
    csv_pending_stage = csv_stage.is_dir()
    csv_queue_before_retry = queue_bytes(csv_root)
    csv_retry_adapter = FakeAdapter(
        [vendor_bar(tue, 0.7)], conid=92102)
    csv_refused = reconcile.reconcile_one(
        csv_retry_adapter, FakePacer(), csv_root, csv_row,
        run_id="changed-legacy-csv-recovery-refused-selftest")
    csv_refused_bytes = csv_path.read_bytes()
    csv_refused_stage = csv_stage.is_dir()
    csv_queue_after_refusal = queue_bytes(csv_root)
    storage._atomic_write_bytes(csv_path, csv_before)
    csv_restored_adapter = FakeAdapter(
        [vendor_bar(tue, 0.7)], conid=92102)
    csv_recovered = reconcile.reconcile_one(
        csv_restored_adapter, FakePacer(), csv_root, csv_row,
        run_id="changed-legacy-csv-recovery-restored-selftest")
    check("normal and recovery CSV cleanup retain changed bytes as debt",
          csv_changed_stats.get("sha256")
          == hashlib.sha256(csv_changed).hexdigest()
          and csv_pending.get("status") == "ambiguous"
          and csv_pending.get("commit_state") == "recovery_required"
          and csv_pending_stage
          and csv_refused.get("status") == "unresolved"
          and csv_refused.get("request_count") == 0
          and not csv_retry_adapter.fetch_calls
          and csv_refused_stage
          and csv_refused_bytes == csv_changed
          and csv_queue_after_refusal == csv_queue_before_retry
          and csv_recovered.get("status") == "stale"
          and csv_recovered.get("request_count") == 0
          and not csv_restored_adapter.fetch_calls
          and not csv_path.exists()
          and not csv_stage.exists()
          and not audit.queue_snapshot(csv_root),
          repr((csv_pending, csv_refused, csv_recovered)))
