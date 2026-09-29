#!/usr/bin/env python3
"""Offline G1 and mutation battery for the Row 79 schedule generator."""

from __future__ import annotations

import copy
from datetime import datetime
from pathlib import Path

import gen_session_schedule as schedule
import snapshot_session_schedule_evidence as snapshot


SOURCE = Path(schedule.__file__).resolve()
VALID_THROUGH = "2028-12-31"
CHECKS = 0


def check(label: str, condition: bool) -> None:
    global CHECKS
    if not condition:
        raise AssertionError(label)
    CHECKS += 1
    print(f"[PASS] {label}")


def expect_value_error(label: str, function, contains: str) -> None:
    try:
        function()
    except ValueError as exc:
        check(label, contains in str(exc))
    else:
        raise AssertionError(f"{label}: expected ValueError")


def load_mutant(old: str, new: str) -> dict:
    source = SOURCE.read_text(encoding="utf-8")
    if source.count(old) != 1:
        raise AssertionError(f"mutation anchor count for {old!r} is not one")
    namespace = {
        "__file__": str(SOURCE),
        "__name__": "_row79_schedule_mutant",
    }
    exec(compile(source.replace(old, new), str(SOURCE), "exec"), namespace)
    return namespace


def mutation_turns_red(label: str, old: str, new: str) -> None:
    namespace = load_mutant(old, new)
    special = namespace["load_and_check_special_closures"]()
    evidence = namespace["load_and_check_evidence"]()
    rows = namespace["generate_rows"](
        namespace["COVERAGE_START"], VALID_THROUGH, special)
    expect_value_error(
        label,
        lambda: namespace["check_close_time_breadth_obligations"](
            rows, evidence),
        "G1 close-breadth",
    )


def main() -> int:
    input_report = schedule.check_inputs()
    evidence = schedule.load_and_check_evidence()
    special = schedule.load_and_check_special_closures()
    rows = schedule.generate_rows(schedule.COVERAGE_START, VALID_THROUGH, special)

    check("Appendix J inputs pass", input_report["status"] == "ok")
    check(
        "evidence denominator is nonempty",
        evidence["close_time_evidence"]["date_count"] > 3700,
    )
    statuses = schedule._validate_rows(
        rows, schedule.COVERAGE_START, VALID_THROUGH)
    check("calendar-date set is complete", sum(statuses.values()) == len(rows))

    crosschecks = schedule._check_crosschecks(rows, evidence, special)
    check(
        "EARLY_CLOSE_DATES identity is 19/19",
        crosschecks["early_close_identity_count"] == 19,
    )
    anomaly_by_date = {
        item["date"]: item
        for item in crosschecks["below_threshold_close_anomalies"]
    }
    for value, expected in schedule.REAL_CLOSE_BREADTH_PINS.items():
        report = anomaly_by_date.get(value)
        expected_tickers = expected["late_tickers"]
        actual_tickers = {
            item["ticker"]: item["final_nonzero_et"]
            for item in report["late_tickers"]
        } if report else None
        check(
            f"{value} is recorded, below threshold, and non-convicting",
            report is not None
            and report["convicts"] is False
            and report["denominator_ticker_count"] == expected["denominator"]
            and report["threshold"] == expected["threshold"]
            and actual_tickers == expected_tickers,
        )
    check(
        "below-threshold anomaly totals are pinned at 30 dates / 33 tickers",
        crosschecks["below_threshold_close_anomaly_dates"] == 30
        and crosschecks["below_threshold_close_anomaly_tickers"] == 33,
    )

    g1 = schedule.check_g1_obligations(rows, special, evidence)
    check("G1 obligation denominator is 110", g1["obligation_count"] == 110)
    check(
        "G1 breadth denominator contributes eleven obligations",
        g1["close_time_breadth"]["obligation_count"] == 11,
    )
    check(
        "G1 registers all five named close-breadth mutation kills",
        g1["close_time_breadth"]["mutation_kill_names"] == [
            "floor_3_to_4",
            "divisor_20_to_21",
            "inclusive_to_strict",
            "ceiling_to_floor",
            "at_after_to_strict_after",
        ],
    )
    check(
        "G1 proves four in-span full closures have zero 1m bars",
        g1["full_closure_zero_bar_count"] == 4,
    )

    duplicate_gap = copy.deepcopy(rows)
    duplicate_gap[1] = copy.deepcopy(duplicate_gap[0])
    expect_value_error(
        "duplicate-plus-gap mutation turns red",
        lambda: schedule._validate_rows(
            duplicate_gap, schedule.COVERAGE_START, VALID_THROUGH),
        "duplicate raw",
    )

    bad_evidence = copy.deepcopy(evidence)
    record = bad_evidence["close_time_evidence"]["dates"][0]
    first_time = next(iter(record["final_nonzero_tickers_by_et"]))
    first_ticker = record["final_nonzero_tickers_by_et"][first_time].split(",")[0]
    record["final_nonzero_tickers_by_et"]["23:59"] = first_ticker
    record["ticker_count_with_nonzero"] += 1
    expect_value_error(
        "ticker duplicated across final-minute groups turns red",
        lambda: schedule._decode_final_nonzero_groups(record, "synthetic"),
        "appears in two groups",
    )

    finals = snapshot._nonzero_daily_final_minutes(
        [datetime(2025, 12, 24, 12, 59), datetime(2025, 12, 24, 13, 1)],
        [10, 0],
        "synthetic.parquet",
    )
    check(
        "zero-volume post-close phantom is ignored",
        finals == {"2025-12-24": 12 * 60 + 59},
    )

    mutation_turns_red(
        "floor 3->4 mutation turns red",
        "CLOSE_TIME_BREADTH_FLOOR = 3",
        "CLOSE_TIME_BREADTH_FLOOR = 4",
    )
    mutation_turns_red(
        "divisor 20->21 mutation turns red",
        "CLOSE_TIME_BREADTH_DIVISOR = 20",
        "CLOSE_TIME_BREADTH_DIVISOR = 21",
    )
    mutation_turns_red(
        "inclusive >= to strict > mutation turns red",
        "return late_count >= threshold",
        "return late_count > threshold",
    )
    mutation_turns_red(
        "ceiling to floor mutation turns red",
        "(denominator + divisor - 1) // divisor",
        "denominator // divisor",
    )
    mutation_turns_red(
        "at/after >= to strict > mutation turns red",
        "for minute, ticker in decoded if minute >= claimed",
        "for minute, ticker in decoded if minute > claimed",
    )

    table, report = schedule.build_table(
        source_commit="0" * 40,
        generated_as_of="2026-09-01",
        valid_through=VALID_THROUGH,
    )
    table_again, report_again = schedule.build_table(
        source_commit="0" * 40,
        generated_as_of="2026-09-01",
        valid_through=VALID_THROUGH,
    )
    check("in-memory table generation succeeds", report["status"] == "ok")
    check("in-memory table generation is deterministic", table == table_again)
    check(
        "deterministic table digest is stable",
        report["table_sha256"] == report_again["table_sha256"],
    )

    print(f"ALL PASS ({CHECKS}/{CHECKS}; offline; no bank writes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
