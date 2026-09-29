"""Focused offline checks for Row 51 M1's volatility ingest gate.

Only temp roots and the production ``split_session_bars`` / ``_commit_month``
seams are used.  No adapter, port, listener, network, GUI, or production bank
is touched.  The test is intentionally red until ``VOL_VALUE_GATE`` and both
M1 guards exist in ``stock_ibkr``.
"""

from __future__ import annotations

import hashlib
import inspect
import tempfile
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import stock_ibkr as sk
import stock_storage as ss


FAILS: list[str] = []
CHECKS = 0


def check(name: str, ok: bool, detail: str = "") -> None:
    global CHECKS
    CHECKS += 1
    print(f"[{'PASS' if ok else 'FAIL'}] {name}"
          + ("" if ok else f"  {detail}"))
    if not ok:
        FAILS.append(name)


def counters() -> dict[str, int]:
    return {"non_rth": 0, "invalid": 0, "outside_day": 0}


def raw_bar(day: date, value: float, *, daily: bool = False,
            low: float | None = None, high: float | None = None,
            close: float | None = None, volume: float = 1.0):
    stamp = (day if daily else
             datetime(day.year, day.month, day.day, 14, 0,
                      tzinfo=timezone.utc))
    return SimpleNamespace(
        date=stamp,
        open=value,
        high=value if high is None else high,
        low=value if low is None else low,
        close=value if close is None else close,
        volume=volume,
    )


def split_one(interval: str, bar, day: date):
    count = counters()
    by_day = sk.split_session_bars(
        [bar], frozenset((day,)), count, interval)
    return by_day.get(day, []), count


def month_bars(year: int, month: int, day: int, value: float,
               *, start_minute: int = 0, count: int = 4):
    start = datetime(year, month, day, 9, 30) + timedelta(
        minutes=start_minute)
    return [
        (start + timedelta(minutes=i), value, value, value, value, 0)
        for i in range(count)
    ]


def result_shell() -> dict:
    return {
        "blocked_months": [],
        "dup_existing": 0,
        "conflicts": 0,
        "added": 0,
        "written": 0,
        "months": {},
        "notes": [],
    }


def commit(root: Path, ticker: str, interval: str, ym: tuple[int, int],
           bars, *, mstate=None):
    res = result_shell()
    conflicts = []

    def conflict_sink(*args):
        conflicts.append(args)

    halted = None
    try:
        sk._commit_month(
            root, ticker, interval, ym, list(bars),
            "vol-value-gate-selftest", "SELFTEST", res, conflict_sink,
            conid=81001, mstate=mstate)
    except sk.SeriesHalt as exc:
        halted = exc
    return res, halted, conflicts


def digest(path: Path | None) -> str | None:
    if path is None or not path.exists():
        return None
    return hashlib.sha256(path.read_bytes()).hexdigest()


def month_path(root: Path, ticker: str, year: int, month: int,
               interval: str) -> Path | None:
    return ss.find_month_file(root, ticker, year, month, interval)


def manifest_path(root: Path, ticker: str) -> Path:
    return root / ticker / ss.MANIFEST_NAME


def read_values(path: Path | None) -> list[float]:
    if path is None:
        return []
    rows, _ = ss.read_month_file(path)
    return [float(v) for row in rows for v in row[1:5]]


def fresh_root(base: Path, name: str) -> Path:
    root = base / name / ss.STORAGE_DIR_NAME
    root.mkdir(parents=True, exist_ok=True)
    return root


def run_split_checks() -> None:
    print("=== constants + per-bar conversion gate ==========================")
    check("VOL_VALUE_GATE is enabled",
          getattr(sk, "VOL_VALUE_GATE", None) is True,
          repr(getattr(sk, "VOL_VALUE_GATE", None)))
    check("VOL_HARD_CEILING is exactly 10.0",
          getattr(sk, "VOL_HARD_CEILING", None) == 10.0,
          repr(getattr(sk, "VOL_HARD_CEILING", None)))
    check("UNIT_FLIP_RATIO is exactly 50.0",
          getattr(sk, "UNIT_FLIP_RATIO", None) == 50.0,
          repr(getattr(sk, "UNIT_FLIP_RATIO", None)))

    day = date(2024, 6, 17)
    iv_over, iv_count = split_one(
        "1m-iv", raw_bar(day, 10.1), day)
    check("IV: an internally valid OHLC bar over 10 is rejected",
          not iv_over and iv_count["invalid"] == 1,
          f"bars={iv_over!r} counters={iv_count!r}")

    hv_over, hv_count = split_one(
        "1d-hvol", raw_bar(day, 10.1, daily=True), day)
    check("HVOL: an internally valid OHLC bar over 10 is rejected",
          not hv_over and hv_count["invalid"] == 1,
          f"bars={hv_over!r} counters={hv_count!r}")

    iv_edge, iv_edge_count = split_one(
        "1m-iv", raw_bar(day, 10.0), day)
    hv_edge, hv_edge_count = split_one(
        "1d-hvol", raw_bar(day, 10.0, daily=True), day)
    check("ratio ceiling is strict: IV and HVOL at exactly 10 are accepted",
          (len(iv_edge) == 1 and len(hv_edge) == 1
           and iv_edge_count["invalid"] == 0
           and hv_edge_count["invalid"] == 0),
          f"iv={iv_edge!r}/{iv_edge_count!r} "
          f"hvol={hv_edge!r}/{hv_edge_count!r}")

    trades, trades_count = split_one(
        "1m", raw_bar(day, 1_000.0, volume=100.0), day)
    bidask, bidask_count = split_one(
        "1m-bidask", raw_bar(day, 1_000.0), day)
    check("TRADES and BID_ASK large prices remain untouched",
          (len(trades) == 1 and len(bidask) == 1
           and trades[0][1:5] == (1_000.0,) * 4
           and bidask[0][1:5] == (1_000.0,) * 4
           and trades_count["invalid"] == 0
           and bidask_count["invalid"] == 0),
          f"trades={trades!r}/{trades_count!r} "
          f"bidask={bidask!r}/{bidask_count!r}")

    negative, negative_count = split_one(
        "1m-iv",
        raw_bar(day, 0.2, low=-0.3, high=0.3, close=-0.2), day)
    check("existing negative volatility rejection remains intact",
          not negative and negative_count["invalid"] == 1,
          f"bars={negative!r} counters={negative_count!r}")


def run_commit_checks(base: Path) -> None:
    print("=== prior-month median gate at the shared commit seam ============")

    exact = fresh_root(base, "exact-50x")
    jan = month_bars(2024, 1, 2, 0.1)
    feb_5 = month_bars(2024, 2, 1, 5.0)
    seed_res, seed_halt, _ = commit(exact, "EXACT", "1m-iv", (2024, 1), jan)
    prior = month_path(exact, "EXACT", 2024, 1, "1m-iv")
    manifest = manifest_path(exact, "EXACT")
    before_prior = digest(prior)
    before_manifest = digest(manifest)
    target_res, target_halt, _ = commit(
        exact, "EXACT", "1m-iv", (2024, 2), feb_5)
    target = month_path(exact, "EXACT", 2024, 2, "1m-iv")
    check("fixture: prior 0.1 month committed cleanly",
          seed_halt is None and seed_res["written"] == 1
          and before_prior is not None and before_manifest is not None,
          f"res={seed_res!r} halt={seed_halt!r}")
    check("exact 50x incoming median raises SeriesHalt",
          isinstance(target_halt, sk.SeriesHalt),
          f"res={target_res!r} halt={target_halt!r}")
    check("exact 50x halt writes no target month and mutates no prior/manifest",
          (target is None and digest(prior) == before_prior
           and digest(manifest) == before_manifest),
          f"target={target!r} prior={digest(prior)!r}/{before_prior!r} "
          f"manifest={digest(manifest)!r}/{before_manifest!r}")

    float_edge = fresh_root(base, "float-exact-50x")
    commit(float_edge, "FLOAT", "1m-iv", (2024, 1),
           month_bars(2024, 1, 2, 0.07))
    float_res, float_halt, _ = commit(
        float_edge, "FLOAT", "1m-iv", (2024, 2),
        month_bars(2024, 2, 1, 3.5))
    check("mathematical 50x stays inclusive across binary float rounding",
          (isinstance(float_halt, sk.SeriesHalt)
           and month_path(float_edge, "FLOAT", 2024, 2, "1m-iv") is None),
          f"halt={float_halt!r} res={float_res!r} "
          f"threshold={50.0 * 0.07!r}")

    partial = fresh_root(base, "partial-target")
    commit(partial, "PART", "1m-iv", (2024, 1), jan)
    partial_rows = month_bars(2024, 2, 1, 0.1)
    commit(partial, "PART", "1m-iv", (2024, 2), partial_rows)
    part_prior = month_path(partial, "PART", 2024, 1, "1m-iv")
    part_target = month_path(partial, "PART", 2024, 2, "1m-iv")
    part_manifest = manifest_path(partial, "PART")
    hashes_before = (digest(part_prior), digest(part_target),
                     digest(part_manifest))
    new_rows = month_bars(2024, 2, 2, 5.0)
    part_res, part_halt, _ = commit(
        partial, "PART", "1m-iv", (2024, 2), new_rows)
    hashes_after = (digest(part_prior), digest(part_target),
                    digest(part_manifest))
    check("50x halt leaves existing prior, partial target, and manifest bytes identical",
          isinstance(part_halt, sk.SeriesHalt)
          and hashes_after == hashes_before,
          f"halt={part_halt!r} before={hashes_before!r} "
          f"after={hashes_after!r} res={part_res!r}")

    below = fresh_root(base, "below-50x")
    commit(below, "BELOW", "1m-iv", (2024, 1), jan)
    below_res, below_halt, _ = commit(
        below, "BELOW", "1m-iv", (2024, 2),
        month_bars(2024, 2, 1, 4.9))
    below_path = month_path(below, "BELOW", 2024, 2, "1m-iv")
    check("49x incoming median remains accepted",
          (below_halt is None and below_res["written"] == 1
           and read_values(below_path)
           and max(read_values(below_path)) == 4.9),
          f"halt={below_halt!r} res={below_res!r} "
          f"values={read_values(below_path)!r}")

    fresh = fresh_root(base, "fresh-series")
    fresh_res, fresh_halt, _ = commit(
        fresh, "FRESH", "1m-iv", (2024, 2), feb_5)
    fresh_path = month_path(fresh, "FRESH", 2024, 2, "1m-iv")
    check("fresh 5.0 series uses ceiling-only and commits",
          fresh_halt is None and fresh_res["written"] == 1
          and bool(read_values(fresh_path)),
          f"halt={fresh_halt!r} res={fresh_res!r}")

    gap = fresh_root(base, "calendar-gap")
    commit(gap, "GAP", "1m-iv", (2024, 1), jan)
    gap_manifest = manifest_path(gap, "GAP")
    gap_before = digest(gap_manifest)
    gap_res, gap_halt, _ = commit(
        gap, "GAP", "1m-iv", (2024, 4),
        month_bars(2024, 4, 1, 5.0))
    check("greatest earlier committed month governs across a calendar gap",
          (isinstance(gap_halt, sk.SeriesHalt)
           and month_path(gap, "GAP", 2024, 4, "1m-iv") is None
           and digest(gap_manifest) == gap_before),
          f"halt={gap_halt!r} res={gap_res!r}")

    lag = fresh_root(base, "manifest-lag")
    commit(lag, "LAG", "1m-iv", (2024, 1), jan)
    lag_manifest = ss.load_manifest(lag / "LAG")
    lag_manifest["intervals"]["1m-iv"]["months"] = {}
    ss.save_manifest(lag / "LAG", lag_manifest)
    lag_res, lag_halt, _ = commit(
        lag, "LAG", "1m-iv", (2024, 4),
        month_bars(2024, 4, 1, 5.0))
    check("month-file authority supplies the prior baseline when manifest lags",
          (isinstance(lag_halt, sk.SeriesHalt)
           and month_path(lag, "LAG", 2024, 4, "1m-iv") is None),
          f"halt={lag_halt!r} res={lag_res!r}")

    decoy = fresh_root(base, "greatest-prior-and-decoys")
    commit(decoy, "DECOY", "1m-iv", (2024, 1),
           month_bars(2024, 1, 2, 0.2))
    commit(decoy, "DECOY", "1m-iv", (2024, 3),
           month_bars(2024, 3, 1, 0.1))
    commit(decoy, "DECOY", "1m", (2024, 4),
           month_bars(2024, 4, 1, 1_000.0))
    commit(decoy, "OTHER", "1m-iv", (2024, 4),
           month_bars(2024, 4, 1, 0.01))
    decoy_state = {
        "manifest": ss.load_manifest(decoy / "DECOY"),
        "since_save": 0,
    }
    decoy_manifest_before = deepcopy(decoy_state["manifest"])
    decoy_res, decoy_halt, _ = commit(
        decoy, "DECOY", "1m-iv", (2024, 5),
        month_bars(2024, 5, 1, 5.0), mstate=decoy_state)
    check("greatest exact-series prior wins over older and decoy months",
          (isinstance(decoy_halt, sk.SeriesHalt)
           and "2024-03" in str(decoy_halt)
           and month_path(decoy, "DECOY", 2024, 5, "1m-iv") is None
           and decoy_state["manifest"] == decoy_manifest_before),
          f"halt={decoy_halt!r} res={decoy_res!r} "
          f"manifest_changed={decoy_state['manifest'] != decoy_manifest_before}")

    missing = fresh_root(base, "missing-claimed-prior")
    commit(missing, "MISS", "1m-iv", (2024, 1), jan)
    missing_prior = month_path(missing, "MISS", 2024, 1, "1m-iv")
    assert missing_prior is not None
    missing_prior.unlink()  # exact temp-root fixture; manifest still claims present
    missing_res, missing_halt, _ = commit(
        missing, "MISS", "1m-iv", (2024, 2),
        month_bars(2024, 2, 1, 0.2))
    check("a manifest-claimed missing prior file fails closed",
          (isinstance(missing_halt, sk.SeriesHalt)
           and "file is missing" in str(missing_halt)
           and month_path(missing, "MISS", 2024, 2, "1m-iv") is None),
          f"halt={missing_halt!r} res={missing_res!r}")

    zero = fresh_root(base, "zero-prior")
    commit(zero, "ZERO", "1m-iv", (2024, 1),
           month_bars(2024, 1, 2, 0.0))
    zero_res, zero_halt, _ = commit(
        zero, "ZERO", "1m-iv", (2024, 2), feb_5)
    zero_path = month_path(zero, "ZERO", 2024, 2, "1m-iv")
    check("zero/nonpositive committed prior median halts fail-closed",
          isinstance(zero_halt, sk.SeriesHalt) and zero_path is None
          and "non-positive" in str(zero_halt),
          f"halt={zero_halt!r} res={zero_res!r}")

    duplicate = fresh_root(base, "duplicate-only")
    dup_rows = month_bars(2024, 1, 2, 0.1)
    commit(duplicate, "DUP", "1m-iv", (2024, 1), dup_rows)
    dup_path = month_path(duplicate, "DUP", 2024, 1, "1m-iv")
    dup_manifest = manifest_path(duplicate, "DUP")
    dup_before = (digest(dup_path), digest(dup_manifest))
    dup_res, dup_halt, dup_conflicts = commit(
        duplicate, "DUP", "1m-iv", (2024, 1), dup_rows)
    dup_after = (digest(dup_path), digest(dup_manifest))
    check("duplicate-only/no-add commit stays an unchanged no-op",
          (dup_halt is None and dup_res["written"] == 0
           and dup_res["added"] == 0
           and dup_res["dup_existing"] == len(dup_rows)
           and not dup_conflicts and dup_before == dup_after),
          f"halt={dup_halt!r} res={dup_res!r} "
          f"conflicts={dup_conflicts!r} hashes={dup_before!r}/{dup_after!r}")

    hvol = fresh_root(base, "hvol-commit")
    commit(hvol, "HV", "1d-hvol", (2024, 1),
           month_bars(2024, 1, 2, 0.1, count=1))
    hv_res, hv_halt, _ = commit(
        hvol, "HV", "1d-hvol", (2024, 2),
        month_bars(2024, 2, 1, 5.0, count=1))
    check("unit-flip commit gate covers HVOL as well as IV",
          isinstance(hv_halt, sk.SeriesHalt)
          and month_path(hvol, "HV", 2024, 2, "1d-hvol") is None,
          f"halt={hv_halt!r} res={hv_res!r}")


def run_wiring_check() -> None:
    print("=== serial/pipeline shared seam ================================")
    fill_src = inspect.getsource(sk._fill_series_inner)
    parallel_src = inspect.getsource(sk.gap_fill_parallel)
    serial_src = inspect.getsource(sk._run_days)
    pipeline_src = inspect.getsource(sk._run_days_pipelined)
    serial_params = inspect.signature(sk._run_days).parameters
    pipeline_params = inspect.signature(sk._run_days_pipelined).parameters
    check("serial and pipeline both receive the flush backed by _commit_month",
          ("_commit_month(" in fill_src
           and "flush" in serial_params and "flush" in pipeline_params
           and "_make_session_processor(" in serial_src
           and "_make_session_processor(" in pipeline_src),
          "the focused behavioral checks call the same _commit_month seam")
    check("ratio-kind series are excluded from out-of-order date splitting",
          (not sk._date_split_series_safe([("IV", "1m-iv")])
           and not sk._date_split_series_safe([("HV", "1d-hvol")])
           and sk._date_split_series_safe([("PX", "1m")])
           and "_date_split_series_safe(series)" in parallel_src
           and parallel_src.index("_date_split_series_safe(series)")
           < parallel_src.index("partition_series_by_months(")),
          "ratio commits must remain chronological while price splitting stays live")


def main() -> int:
    run_split_checks()
    with tempfile.TemporaryDirectory(prefix="vol_value_gate_selftest_") as td:
        run_commit_checks(Path(td))
    run_wiring_check()
    print(f"\n{CHECKS} checks, {len(FAILS)} failed")
    if FAILS:
        print("FAILED:")
        for name in FAILS:
            print(f"  - {name}")
        return 1
    print("ALL PASS (temp roots; no ports/network/GUI/production bank)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
