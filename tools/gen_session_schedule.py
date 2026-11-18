#!/usr/bin/env python3
"""Generate and verify Row 79's committed NYSE regular-session schedule.

The generator is Python-stdlib-only and consumes only committed repository inputs:
Appendix J's dated rules, the cited 1980+ special-closure list, and the reduced bank
evidence snapshot.  It never fetches, imports a calendar package, or writes the bank.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import re
import tempfile
from collections import Counter
from datetime import date, timedelta
from pathlib import Path


COVERAGE_START = "1980-01-02"
OPEN_ENDED = None
SPECIAL_CLOSURES_PATH = Path(__file__).with_name(
    "special_closures_1980_plus.json")
EVIDENCE_PATH = Path(__file__).with_name("session_schedule_evidence.json")
PROJECT_ROOT = Path(__file__).resolve().parent.parent
TABLE_PATH = PROJECT_ROOT / "engine" / "session_schedule_table.json"
MARKET_CALENDAR_PATH = PROJECT_ROOT / "engine" / "market_calendar.py"
SESSION_FAMILY = "xnys_regular"
MIN_GENERATION_RUNWAY_DAYS = 180
TABLE_SCHEMA_VERSION = 1
SOURCE_COMMIT_RE = re.compile(r"[0-9a-f]{40}\Z")
CLOSE_TIME_BREADTH_FLOOR = 3
CLOSE_TIME_BREADTH_DIVISOR = 20

REAL_CLOSE_BREADTH_PINS = {
    "2011-11-25": {
        "claimed_close_et": "13:00",
        "denominator": 412,
        "late_tickers": {"ABT": "13:00"},
        "threshold": 21,
    },
    "2018-07-03": {
        "claimed_close_et": "13:00",
        "denominator": 464,
        "late_tickers": {"GOOG": "15:54", "KDP": "13:12"},
        "threshold": 24,
    },
    "2019-11-29": {
        "claimed_close_et": "13:00",
        "denominator": 477,
        "late_tickers": {"GOOG": "15:50", "VRT": "13:15"},
        "threshold": 24,
    },
    "2023-11-24": {
        "claimed_close_et": "13:00",
        "denominator": 495,
        "late_tickers": {"EXPD": "13:20", "GOOG": "15:42"},
        "threshold": 25,
    },
}

SPECIAL_CLOSURE_KINDS = (
    "full_closure",
    "early_close_unscheduled",
    "late_close_unscheduled",
)

# Appendix J2.  ``algorithm`` and its parameters are generator-facing tokens;
# ``effective_start`` and ``effective_end`` are inclusive calendar-date bounds.
HOLIDAY_RULES = (
    {
        "rule_id": "weekend",
        "algorithm": "weekend",
        "effective_start": COVERAGE_START,
        "effective_end": OPEN_ENDED,
        "definition": "Saturday and Sunday are never sessions",
        "citation": "[REG] (5-day week since 1952-09-29)",
    },
    {
        "rule_id": "shift_sunday",
        "algorithm": "observe_sunday_next_monday",
        "effective_start": COVERAGE_START,
        "effective_end": OPEN_ENDED,
        "definition": "Holiday on Sunday → following Monday closed",
        "citation": (
            "[REG] \"Traditionally, when a holiday falls on a Sunday, the NYSE "
            "closes the succeeding Monday\""
        ),
    },
    {
        "rule_id": "shift_saturday",
        "algorithm": "observe_saturday_previous_friday_unless_period_end",
        "effective_start": COVERAGE_START,
        "effective_end": OPEN_ENDED,
        "definition": (
            "Holiday on Saturday → preceding Friday closed UNLESS that Friday "
            "ends a monthly or yearly accounting period"
        ),
        "citation": (
            "[REG]; NYSE Rule 51 policy adopted 1959-07-03. Consequence: New "
            "Year's Day on Saturday never closes the prior Friday (Dec 31 is "
            "always a yearly period end — e.g. 2021-12-31 OPEN) while "
            "Christmas-on-Saturday closes Dec 24 (e.g. 2021-12-24 CLOSED) and "
            "July-4-on-Saturday closes Jul 3 (e.g. 2020-07-03, 2026-07-03 "
            "CLOSED). BOTH polarity cases are mandatory G1 pins."
        ),
    },
    {
        "rule_id": "hol_new_years",
        "algorithm": "fixed_date",
        "month": 1,
        "day": 1,
        "shift_policy": "standard",
        "effective_start": COVERAGE_START,
        "effective_end": OPEN_ENDED,
        "definition": "Jan 1 (+shift rules)",
        "citation": "[REG] \"Closed every year\"",
    },
    {
        "rule_id": "hol_mlk",
        "algorithm": "nth_weekday",
        "month": 1,
        "weekday": "monday",
        "ordinal": 3,
        "shift_policy": None,
        "effective_start": "1998-01-19",
        "effective_end": OPEN_ENDED,
        "definition": "Third Monday of January",
        "citation": "[REG] \"Closed all day beginning in 1998\"",
    },
    {
        "rule_id": "hol_washington",
        "algorithm": "nth_weekday",
        "month": 2,
        "weekday": "monday",
        "ordinal": 3,
        "shift_policy": None,
        "effective_start": COVERAGE_START,
        "effective_end": OPEN_ENDED,
        "definition": "Third Monday of February",
        "citation": "[REG] (Mondays since 1971)",
    },
    {
        "rule_id": "hol_good_friday",
        "algorithm": "western_good_friday",
        "shift_policy": None,
        "effective_start": COVERAGE_START,
        "effective_end": OPEN_ENDED,
        "definition": "Good Friday (Western/Gregorian computus)",
        "citation": (
            "[REG] \"Closed every year except 1898, 1906 and 1907\" "
            "(exceptions pre-window)"
        ),
    },
    {
        "rule_id": "hol_memorial",
        "algorithm": "last_weekday",
        "month": 5,
        "weekday": "monday",
        "shift_policy": None,
        "effective_start": COVERAGE_START,
        "effective_end": OPEN_ENDED,
        "definition": "Last Monday of May",
        "citation": "[REG] (Mondays since 1971)",
    },
    {
        "rule_id": "hol_juneteenth",
        "algorithm": "fixed_date",
        "month": 6,
        "day": 19,
        "shift_policy": "standard",
        "effective_start": "2022-06-20",
        "effective_end": OPEN_ENDED,
        "definition": "June 19 (+shift rules)",
        "citation": (
            "Bloomberg 2022-06-13 explainer (first market observance "
            "2022-06-20; June 19 2022 fell Sunday)"
        ),
    },
    {
        "rule_id": "hol_independence",
        "algorithm": "fixed_date",
        "month": 7,
        "day": 4,
        "shift_policy": "standard",
        "effective_start": COVERAGE_START,
        "effective_end": OPEN_ENDED,
        "definition": "July 4 (+shift rules)",
        "citation": "[REG]",
    },
    {
        "rule_id": "hol_labor",
        "algorithm": "nth_weekday",
        "month": 9,
        "weekday": "monday",
        "ordinal": 1,
        "shift_policy": None,
        "effective_start": COVERAGE_START,
        "effective_end": OPEN_ENDED,
        "definition": "First Monday of September",
        "citation": "[REG]",
    },
    {
        "rule_id": "hol_election",
        "algorithm": "first_tuesday_after_first_monday",
        "month": 11,
        "presidential_year_modulus": 4,
        "presidential_year_remainder": 0,
        "shift_policy": None,
        "effective_start": COVERAGE_START,
        "effective_end": "1980-11-04",
        "definition": (
            "First Tuesday after first Monday of November, PRESIDENTIAL "
            "election years only"
        ),
        "citation": (
            "[REG] \"Closed presidential election years only, 1972–1980\""
        ),
    },
    {
        "rule_id": "hol_thanksgiving",
        "algorithm": "nth_weekday",
        "month": 11,
        "weekday": "thursday",
        "ordinal": 4,
        "shift_policy": None,
        "effective_start": COVERAGE_START,
        "effective_end": OPEN_ENDED,
        "definition": "Fourth Thursday of November",
        "citation": "[REG]",
    },
    {
        "rule_id": "hol_christmas",
        "algorithm": "fixed_date",
        "month": 12,
        "day": 25,
        "shift_policy": "standard",
        "effective_start": COVERAGE_START,
        "effective_end": OPEN_ENDED,
        "definition": "Dec 25 (+shift rules)",
        "citation": "[REG]",
    },
)

# Appendix J2 explicitly makes these former holidays open obligations throughout
# the in-window span.  Keeping them as recurrence-shaped inputs lets G1 enumerate
# representative open obligations instead of relying on prose.
EXPLICIT_NON_HOLIDAY_RULES = (
    {
        "rule_id": "open_lincolns_birthday",
        "algorithm": "fixed_date",
        "month": 2,
        "day": 12,
        "effective_start": COVERAGE_START,
        "effective_end": OPEN_ENDED,
        "definition": "Lincoln's Birthday is not an in-window NYSE holiday",
        "citation": "[REG] (full closures ended 1953)",
    },
    {
        "rule_id": "open_columbus_day",
        "algorithm": "nth_weekday",
        "month": 10,
        "weekday": "monday",
        "ordinal": 2,
        "effective_start": COVERAGE_START,
        "effective_end": OPEN_ENDED,
        "definition": "Columbus Day is not an in-window NYSE holiday",
        "citation": "[REG] (full closures ended 1953)",
    },
    {
        "rule_id": "open_veterans_day",
        "algorithm": "fixed_date",
        "month": 11,
        "day": 11,
        "effective_start": COVERAGE_START,
        "effective_end": OPEN_ENDED,
        "definition": "Veterans Day is not an in-window NYSE holiday",
        "citation": (
            "[REG] (full closures ended 1953; the 11:00 two-minute silence "
            "1954–2006 was not a closure)"
        ),
    },
)

# Appendix J3 standing early-close rules.  Bounds are inclusive.
EARLY_CLOSE_RULES = (
    {
        "rule_id": "ec_thanksgiving_friday_1400",
        "algorithm": "day_after_rule",
        "base_rule_id": "hol_thanksgiving",
        "close_et": "14:00",
        "effective_start": "1992-11-27",
        "effective_end": "1992-11-27",
        "definition": "Day after Thanksgiving closes 14:00",
        "citation": (
            "[REG] \"Nov. 27, 1992 (Fri) Closed at 2:00 pm. Day after "
            "Thanksgiving\" (first ever)"
        ),
    },
    {
        "rule_id": "ec_thanksgiving_friday",
        "algorithm": "day_after_rule",
        "base_rule_id": "hol_thanksgiving",
        "close_et": "13:00",
        "effective_start": "1993-11-26",
        "effective_end": OPEN_ENDED,
        "definition": "Day after Thanksgiving closes 13:00",
        "citation": (
            "[REG] (13:00 every year from 1993; e.g. 1993-11-26, "
            "1994-11-25, …, 2010-11-26) + NYSE Group holiday-calendar "
            "releases (businesswire 2020-12-28, 2022-12-21, 2025-12-23)"
        ),
    },
    {
        "rule_id": "ec_christmas_eve",
        "algorithm": "fixed_date_if_trading_day",
        "month": 12,
        "day": 24,
        "close_et": "13:00",
        "effective_start": "1996-12-24",
        "effective_end": OPEN_ENDED,
        "definition": (
            "Dec 24 closes 13:00 whenever it is a trading day (weekday not "
            "consumed by the Christmas observation shift)"
        ),
        "citation": (
            "[REG] (1996, 1997, 1998, 2001, 2002, 2003, 2007, 2008, "
            "2009 instances) + NYSE calendar releases through 2026-12-24"
        ),
    },
    {
        "rule_id": "ec_july_3",
        "algorithm": "fixed_date_if_trading_day_and_next_day_weekday",
        "month": 7,
        "day": 3,
        "next_month": 7,
        "next_day": 4,
        "close_et": "13:00",
        "effective_start": "1995-07-03",
        "effective_end": OPEN_ENDED,
        "definition": (
            "July 3 closes 13:00 whenever it is a trading day and July 4 "
            "is a weekday"
        ),
        "citation": (
            "[REG] (1995-07-03 first; 1997, 2000, 2001, 2003, 2006, 2007, "
            "2008 instances) + NYSE calendar releases (2019-07-03, "
            "2023–2025 instances)"
        ),
    },
)

DATED_EARLY_CLOSE_EXCEPTIONS = (
    {
        "date": "1990-12-24",
        "close_et": "14:00",
        "reason": "Christmas Eve, pre-rule era",
        "citation": "[REG]",
    },
    {
        "date": "1991-12-24",
        "close_et": "14:00",
        "reason": "Christmas Eve, pre-rule era",
        "citation": "[REG]",
    },
    {
        "date": "1992-12-24",
        "close_et": "14:00",
        "reason": "Christmas Eve, pre-rule era",
        "citation": "[REG]",
    },
    {
        "date": "1996-07-05",
        "close_et": "13:00",
        "reason": "Day after Independence Day (July 4 Thursday)",
        "citation": "[REG]",
    },
    {
        "date": "2002-07-05",
        "close_et": "13:00",
        "reason": (
            "Day after Independence Day (practice not continued: "
            "2013-07-05 full session)"
        ),
        "citation": "[REG]",
    },
    {
        "date": "1997-12-26",
        "close_et": "13:00",
        "reason": "Friday after Christmas (Christmas Thursday)",
        "citation": "[REG]",
    },
    {
        "date": "2003-12-26",
        "close_et": "13:00",
        "reason": (
            "Friday after Christmas (practice not continued: "
            "2008-12-26 full session)"
        ),
        "citation": "[REG]",
    },
    {
        "date": "1999-12-31",
        "close_et": "13:00",
        "reason": "New Year's Eve, Y2K changeover precaution",
        "citation": (
            "[REG] entry (text quoted by Benzinga \"NYSE Braces For Potential "
            "Y2K Chaos\", 2021-12)"
        ),
    },
)

CHRISTMAS_EVE_GAP_OBLIGATIONS = (
    {"date": "1993-12-24", "expected": "closed_holiday"},
    {"date": "1994-12-24", "expected": "closed_weekend"},
    {"date": "1995-12-24", "expected": "closed_weekend"},
    {"date": "1999-12-24", "expected": "closed_holiday"},
    {"date": "2000-12-24", "expected": "closed_weekend"},
    {"date": "2004-12-24", "expected": "closed_holiday"},
    {"date": "2005-12-24", "expected": "closed_weekend"},
    {"date": "2010-12-24", "expected": "closed_holiday"},
    {"date": "2016-12-24", "expected": "closed_weekend"},
    {"date": "2017-12-24", "expected": "closed_weekend"},
    {"date": "2021-12-24", "expected": "closed_holiday"},
    {"date": "2022-12-24", "expected": "closed_weekend"},
    {"date": "2023-12-24", "expected": "closed_weekend"},
)

# CODEX-79-A0-J3-1 was confirmed by Claude on 2026-08-31 and Appendix J3 was
# corrected with attribution.  Keep the empty tuple in the digest/report schema
# so any future planner finding must be explicit and generation can fail closed.
PLANNER_INPUT_FINDINGS = ()

# Appendix J4.  The regular close is constant across coverage.  The open-time
# transition is retained because intraday session windows consume it.
REGULAR_SESSION = {
    "regular_close_et": "16:00",
    "daily_settle_floor_close_et": "16:00",
    "daily_settle_margin_minutes": 20,
    "timezone": "America/New_York",
    "regular_close_effective_start": COVERAGE_START,
    "regular_close_effective_end": OPEN_ENDED,
    "citation": (
        "Close extended 15:30→16:00 effective 1974-10-01 (pre-window; "
        "Markets Media \"Flashback Friday: NYSE Trading Hours\"); no "
        "regular-close change since"
    ),
}

REGULAR_OPEN_TRANSITIONS = (
    {
        "open_et": "10:00",
        "effective_start": "1980-01-02",
        "effective_end": "1985-09-27",
        "citation": (
            "Washington Post 1985-09-29 \"Stock Market To Get Off to Earlier "
            "Start\"; Benzinga \"NYSE Moves Opening Trading Bell To 9:30 A.M.\""
        ),
    },
    {
        "open_et": "09:30",
        "effective_start": "1985-09-30",
        "effective_end": OPEN_ENDED,
        "citation": (
            "Washington Post 1985-09-29 \"Stock Market To Get Off to Earlier "
            "Start\"; Benzinga \"NYSE Moves Opening Trading Bell To 9:30 A.M.\""
        ),
    },
)

MANDATORY_PINS = {
    "mlk_span": ("1997-01-20", "1998-01-19"),
    "juneteenth_span": ("2021-06-18", "2021-06-21", "2022-06-20"),
    "election_span": ("1980-11-04", "1984-11-06"),
    "thanksgiving_friday_transition": ("1992-11-27", "1993-11-26"),
    "july_3_adoption": ("1995-07-03",),
    "christmas_eve_adoption": ("1996-12-24",),
    "shift_saturday_polarity": ("2021-12-24", "2021-12-31"),
    "late_close_floor_margin": ("2009-07-02",),
}


DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}\Z")
TIME_RE = re.compile(r"(?:[01]\d|2[0-3]):[0-5]\d\Z")
SPECIAL_ENTRY_KEYS = {"date", "kind", "close_et", "reason", "citation"}
EXPECTED_SPECIAL_COUNTS = {
    "full_closure": 12,
    "early_close_unscheduled": 20,
    "late_close_unscheduled": 1,
}


def _parse_date(value: str, label: str) -> date:
    if not isinstance(value, str) or not DATE_RE.fullmatch(value):
        raise ValueError(f"{label}: expected YYYY-MM-DD")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{label}: invalid calendar date {value!r}") from exc


def _check_time(value: str, label: str) -> None:
    if not isinstance(value, str) or not TIME_RE.fullmatch(value):
        raise ValueError(f"{label}: expected HH:MM")


def _check_citation(record: dict, label: str) -> None:
    citation = record.get("citation")
    if not isinstance(citation, str) or not citation.strip():
        raise ValueError(f"{label}: missing citation")


def _check_span(record: dict, label: str) -> None:
    start = _parse_date(record["effective_start"], f"{label}.effective_start")
    end_raw = record["effective_end"]
    if end_raw is not None:
        end = _parse_date(end_raw, f"{label}.effective_end")
        if end < start:
            raise ValueError(f"{label}: effective_end precedes effective_start")


def _check_rule_set(records: tuple[dict, ...], label: str) -> None:
    ids = []
    for index, record in enumerate(records):
        item_label = f"{label}[{index}]"
        if not isinstance(record, dict):
            raise ValueError(f"{item_label}: expected object")
        rule_id = record.get("rule_id")
        if not isinstance(rule_id, str) or not rule_id:
            raise ValueError(f"{item_label}: missing rule_id")
        if not isinstance(record.get("algorithm"), str):
            raise ValueError(f"{item_label}: missing algorithm")
        _check_span(record, item_label)
        _check_citation(record, item_label)
        ids.append(rule_id)
    duplicates = sorted(rule_id for rule_id, count in Counter(ids).items() if count > 1)
    if duplicates:
        raise ValueError(f"{label}: duplicate rule IDs {duplicates}")


def load_and_check_special_closures(path: Path = SPECIAL_CLOSURES_PATH) -> list[dict]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read special-closure input {path}: {exc}") from exc
    if not isinstance(payload, list):
        raise ValueError("special-closure root must be a JSON array")

    dates = []
    counts = Counter()
    for index, record in enumerate(payload):
        label = f"special[{index}]"
        if not isinstance(record, dict):
            raise ValueError(f"{label}: expected object")
        if set(record) != SPECIAL_ENTRY_KEYS:
            raise ValueError(
                f"{label}: keys must equal {sorted(SPECIAL_ENTRY_KEYS)}")
        parsed = _parse_date(record["date"], f"{label}.date")
        if parsed < date.fromisoformat(COVERAGE_START):
            raise ValueError(f"{label}: date predates coverage")
        kind = record["kind"]
        if kind not in SPECIAL_CLOSURE_KINDS:
            raise ValueError(f"{label}: unknown kind {kind!r}")
        if kind == "full_closure":
            if record["close_et"] is not None:
                raise ValueError(f"{label}: full closure must have null close_et")
        else:
            _check_time(record["close_et"], f"{label}.close_et")
        if not isinstance(record["reason"], str) or not record["reason"].strip():
            raise ValueError(f"{label}: missing reason")
        _check_citation(record, label)
        dates.append(record["date"])
        counts[kind] += 1

    duplicates = sorted(value for value, count in Counter(dates).items() if count > 1)
    if duplicates:
        raise ValueError(f"duplicate special-closure dates: {duplicates}")
    if dict(counts) != EXPECTED_SPECIAL_COUNTS:
        raise ValueError(
            f"special-closure counts {dict(counts)} != {EXPECTED_SPECIAL_COUNTS}")
    late = [record for record in payload if record["kind"] == "late_close_unscheduled"]
    if [(record["date"], record["close_et"]) for record in late] != [
            ("2009-07-02", "16:15")]:
        raise ValueError("the singular 2009-07-02 16:15 late close is not pinned")
    return payload


def check_inputs() -> dict:
    _check_rule_set(HOLIDAY_RULES, "holiday_rules")
    _check_rule_set(EXPLICIT_NON_HOLIDAY_RULES, "explicit_non_holiday_rules")
    _check_rule_set(EARLY_CLOSE_RULES, "early_close_rules")
    if len(HOLIDAY_RULES) != 14:
        raise ValueError("Appendix J2 requires exactly 14 holiday/shift rules")
    if len(EXPLICIT_NON_HOLIDAY_RULES) != 3:
        raise ValueError("Appendix J2 requires exactly three explicit-open rules")
    if len(EARLY_CLOSE_RULES) != 4:
        raise ValueError("Appendix J3 requires exactly four standing early-close rules")
    if tuple(record["rule_id"] for record in HOLIDAY_RULES) != (
        "weekend", "shift_sunday", "shift_saturday", "hol_new_years",
        "hol_mlk", "hol_washington", "hol_good_friday", "hol_memorial",
        "hol_juneteenth", "hol_independence", "hol_labor", "hol_election",
        "hol_thanksgiving", "hol_christmas",
    ):
        raise ValueError("Appendix J2 holiday/shift rule IDs or order drifted")
    if tuple(record["rule_id"] for record in EXPLICIT_NON_HOLIDAY_RULES) != (
        "open_lincolns_birthday", "open_columbus_day", "open_veterans_day",
    ):
        raise ValueError("Appendix J2 explicit-open rule IDs or order drifted")
    if tuple(record["rule_id"] for record in EARLY_CLOSE_RULES) != (
        "ec_thanksgiving_friday_1400", "ec_thanksgiving_friday",
        "ec_christmas_eve", "ec_july_3",
    ):
        raise ValueError("Appendix J3 early-close rule IDs or order drifted")
    for index, record in enumerate(EARLY_CLOSE_RULES):
        _check_time(record["close_et"], f"early_close_rules[{index}].close_et")

    exception_dates = []
    for index, record in enumerate(DATED_EARLY_CLOSE_EXCEPTIONS):
        label = f"dated_exception[{index}]"
        _parse_date(record["date"], f"{label}.date")
        _check_time(record["close_et"], f"{label}.close_et")
        _check_citation(record, label)
        if not isinstance(record.get("reason"), str) or not record["reason"].strip():
            raise ValueError(f"{label}: missing reason")
        exception_dates.append(record["date"])
    if len(exception_dates) != 8 or len(set(exception_dates)) != 8:
        raise ValueError("Appendix J3 requires eight unique dated exceptions")

    gap_dates = []
    for index, record in enumerate(CHRISTMAS_EVE_GAP_OBLIGATIONS):
        label = f"christmas_eve_gap[{index}]"
        _parse_date(record["date"], f"{label}.date")
        if record.get("expected") not in {"closed_holiday", "closed_weekend"}:
            raise ValueError(f"{label}: invalid expected disposition")
        gap_dates.append(record["date"])
    if len(gap_dates) != 13 or len(set(gap_dates)) != 13:
        raise ValueError("Appendix J3 requires 13 unique Christmas-Eve gaps")

    _check_time(REGULAR_SESSION["regular_close_et"], "regular_close_et")
    _check_time(
        REGULAR_SESSION["daily_settle_floor_close_et"],
        "daily_settle_floor_close_et")
    if REGULAR_SESSION["regular_close_et"] != "16:00":
        raise ValueError("Appendix J4 requires a 16:00 regular close")
    if REGULAR_SESSION["daily_settle_floor_close_et"] != "16:00":
        raise ValueError("Appendix J4 requires a 16:00 daily settle floor")
    if REGULAR_SESSION["daily_settle_margin_minutes"] != 20:
        raise ValueError("Appendix J4 requires a 20-minute settle margin")
    if REGULAR_SESSION["timezone"] != "America/New_York":
        raise ValueError("session timezone must be America/New_York")
    _check_citation(REGULAR_SESSION, "regular_session")

    for index, record in enumerate(REGULAR_OPEN_TRANSITIONS):
        label = f"regular_open[{index}]"
        _check_time(record["open_et"], f"{label}.open_et")
        _check_span(record, label)
        _check_citation(record, label)
    if tuple(record["open_et"] for record in REGULAR_OPEN_TRANSITIONS) != (
            "10:00", "09:30"):
        raise ValueError("Appendix J4 regular-open transition is not pinned")

    for pin_name, pin_dates in MANDATORY_PINS.items():
        if not isinstance(pin_dates, tuple) or not pin_dates:
            raise ValueError(f"mandatory pin {pin_name}: expected nonempty tuple")
        for pin_index, pin_date in enumerate(pin_dates):
            _parse_date(pin_date, f"mandatory_pins.{pin_name}[{pin_index}]")
    if PLANNER_INPUT_FINDINGS:
        raise ValueError("unresolved planner input finding blocks generation")

    special = load_and_check_special_closures()
    canonical = {
        "coverage_start": COVERAGE_START,
        "holiday_rules": HOLIDAY_RULES,
        "explicit_non_holiday_rules": EXPLICIT_NON_HOLIDAY_RULES,
        "early_close_rules": EARLY_CLOSE_RULES,
        "dated_early_close_exceptions": DATED_EARLY_CLOSE_EXCEPTIONS,
        "christmas_eve_gap_obligations": CHRISTMAS_EVE_GAP_OBLIGATIONS,
        "regular_session": REGULAR_SESSION,
        "regular_open_transitions": REGULAR_OPEN_TRANSITIONS,
        "mandatory_pins": MANDATORY_PINS,
        "special_closures": special,
    }
    canonical_bytes = json.dumps(
        canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("ascii")
    return {
        "status": "ok",
        "coverage_start": COVERAGE_START,
        "input_digest_scope": "Appendix J1-J4 generator inputs only",
        "holiday_rule_count": len(HOLIDAY_RULES),
        "explicit_non_holiday_rule_count": len(EXPLICIT_NON_HOLIDAY_RULES),
        "early_close_rule_count": len(EARLY_CLOSE_RULES),
        "dated_exception_count": len(DATED_EARLY_CLOSE_EXCEPTIONS),
        "regular_open_transition_count": len(REGULAR_OPEN_TRANSITIONS),
        "planner_input_finding_count": len(PLANNER_INPUT_FINDINGS),
        "planner_input_finding_ids": [
            finding["finding_id"] for finding in PLANNER_INPUT_FINDINGS
        ],
        "special_closure_counts": EXPECTED_SPECIAL_COUNTS,
        "special_closure_count": len(special),
        "input_digest_sha256": hashlib.sha256(canonical_bytes).hexdigest(),
        "generated_schedule": False,
    }


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("ascii")


def _digest_value(value: object) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _minutes(value: str, label: str) -> int:
    _check_time(value, label)
    hour, minute = (int(part) for part in value.split(":"))
    return hour * 60 + minute


def close_time_breadth_threshold(denominator: int) -> int:
    if (isinstance(denominator, bool)
            or not isinstance(denominator, int) or denominator < 1):
        raise ValueError("close-time denominator must be a positive integer")
    divisor = CLOSE_TIME_BREADTH_DIVISOR
    breadth = (denominator + divisor - 1) // divisor
    return max(CLOSE_TIME_BREADTH_FLOOR, breadth)


def close_time_convicts(late_count: int, denominator: int) -> bool:
    if (isinstance(late_count, bool)
            or not isinstance(late_count, int)
            or late_count < 0 or late_count > denominator):
        raise ValueError("late ticker count must be within the denominator")
    threshold = close_time_breadth_threshold(denominator)
    return late_count >= threshold


def _decode_final_nonzero_groups(record: dict, label: str) -> list[tuple[int, str]]:
    groups = record.get("final_nonzero_tickers_by_et")
    if not isinstance(groups, dict) or not groups:
        raise ValueError(f"{label}: final-minute groups must be a nonempty object")
    decoded = []
    seen_tickers = set()
    if list(groups) != sorted(groups):
        raise ValueError(f"{label}: final-minute groups are not time-sorted")
    for time_et, encoded_tickers in groups.items():
        minute = _minutes(time_et, f"{label}.{time_et}")
        if not isinstance(encoded_tickers, str) or not encoded_tickers:
            raise ValueError(f"{label}.{time_et}: ticker group must be nonempty text")
        tickers = encoded_tickers.split(",")
        if tickers != sorted(set(tickers)):
            raise ValueError(
                f"{label}.{time_et}: tickers must be sorted and unique")
        for ticker in tickers:
            if not ticker or ticker != ticker.upper() or "," in ticker:
                raise ValueError(
                    f"{label}.{time_et}: noncanonical ticker {ticker!r}")
            if ticker in seen_tickers:
                raise ValueError(f"{label}: ticker {ticker} appears in two groups")
            seen_tickers.add(ticker)
            decoded.append((minute, ticker))
    count = record.get("ticker_count_with_nonzero")
    if (isinstance(count, bool) or not isinstance(count, int) or count < 1
            or count != len(decoded)):
        raise ValueError(
            f"{label}: ticker denominator {count!r} != decoded {len(decoded)}")
    return decoded


def evaluate_close_time_record(record: dict, claimed_close_et: str) -> dict:
    claimed = _minutes(claimed_close_et, "claimed close")
    decoded = _decode_final_nonzero_groups(
        record, f"close-time evidence {record.get('date', '?')}")
    late_tickers = [
        {
            "final_nonzero_et": f"{minute // 60:02d}:{minute % 60:02d}",
            "ticker": ticker,
        }
        for minute, ticker in decoded if minute >= claimed
    ]
    late_tickers.sort(key=lambda item: (item["ticker"], item["final_nonzero_et"]))
    denominator = len(decoded)
    threshold = close_time_breadth_threshold(denominator)
    return {
        "claimed_close_et": claimed_close_et,
        "date": record["date"],
        "denominator_ticker_count": denominator,
        "late_ticker_count": len(late_tickers),
        "late_tickers": late_tickers,
        "threshold": threshold,
        "convicts": close_time_convicts(len(late_tickers), denominator),
    }


def _nth_weekday(year: int, month: int, weekday: int, ordinal: int) -> date:
    first = date(year, month, 1)
    return first + timedelta(
        days=(weekday - first.weekday()) % 7 + 7 * (ordinal - 1))


def _last_weekday(year: int, month: int, weekday: int) -> date:
    next_month = (date(year + 1, 1, 1) if month == 12
                  else date(year, month + 1, 1))
    last = next_month - timedelta(days=1)
    return last - timedelta(days=(last.weekday() - weekday) % 7)


def _easter(year: int) -> date:
    a = year % 19
    b, c = year // 100, year % 100
    d, e = b // 4, b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = c // 4, c % 4
    ll = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * ll) // 451
    month = (h + ll - 7 * m + 114) // 31
    day = ((h + ll - 7 * m + 114) % 31) + 1
    return date(year, month, day)


def _in_span(value: date, record: dict) -> bool:
    start = date.fromisoformat(record["effective_start"])
    end = (date.max if record["effective_end"] is None
           else date.fromisoformat(record["effective_end"]))
    return start <= value <= end


def _observed_fixed(value: date) -> date | None:
    if value.weekday() == 6:
        return value + timedelta(days=1)
    if value.weekday() == 5:
        prior = value - timedelta(days=1)
        next_day = prior + timedelta(days=1)
        if next_day.month != prior.month:
            return None
        return prior
    return value


def _rule_occurrence(record: dict, year: int, *, shifted: bool = True) -> date | None:
    algorithm = record["algorithm"]
    if algorithm in {"weekend", "observe_sunday_next_monday",
                     "observe_saturday_previous_friday_unless_period_end"}:
        return None
    if algorithm == "fixed_date":
        value = date(year, record["month"], record["day"])
        if shifted and record.get("shift_policy") == "standard":
            value = _observed_fixed(value)
    elif algorithm == "nth_weekday":
        weekday = {"monday": 0, "thursday": 3}[record["weekday"]]
        value = _nth_weekday(year, record["month"], weekday, record["ordinal"])
    elif algorithm == "last_weekday":
        weekday = {"monday": 0}[record["weekday"]]
        value = _last_weekday(year, record["month"], weekday)
    elif algorithm == "western_good_friday":
        value = _easter(year) - timedelta(days=2)
    elif algorithm == "first_tuesday_after_first_monday":
        if year % record["presidential_year_modulus"] \
                != record["presidential_year_remainder"]:
            return None
        first = date(year, record["month"], 1)
        first_monday = first + timedelta(days=(0 - first.weekday()) % 7)
        value = first_monday + timedelta(days=1)
    else:
        raise ValueError(f"unknown rule algorithm {algorithm!r}")
    return value if value is not None and _in_span(value, record) else None


def _holiday_rule_map() -> dict[str, dict]:
    return {record["rule_id"]: record for record in HOLIDAY_RULES}


def _early_rule_occurrence(record: dict, year: int) -> date | None:
    algorithm = record["algorithm"]
    if algorithm == "day_after_rule":
        base = _holiday_rule_map()[record["base_rule_id"]]
        value = _rule_occurrence(base, year)
        value = None if value is None else value + timedelta(days=1)
    elif algorithm == "fixed_date_if_trading_day":
        value = date(year, record["month"], record["day"])
    elif algorithm == "fixed_date_if_trading_day_and_next_day_weekday":
        next_day = date(year, record["next_month"], record["next_day"])
        value = (date(year, record["month"], record["day"])
                 if next_day.weekday() < 5 else None)
    else:
        raise ValueError(f"unknown early-close algorithm {algorithm!r}")
    return value if value is not None and _in_span(value, record) else None


def _regular_open(value: date) -> str:
    for record in REGULAR_OPEN_TRANSITIONS:
        if _in_span(value, record):
            return record["open_et"]
    raise ValueError(f"no regular-open transition covers {value.isoformat()}")


def _date_range(first: date, last: date):
    current = first
    while current <= last:
        yield current
        current += timedelta(days=1)


def generate_rows(first_date: str, valid_through: str, special: list[dict]) -> list[dict]:
    first = _parse_date(first_date, "first_date")
    last = _parse_date(valid_through, "valid_through")
    if last < first:
        raise ValueError("valid_through precedes first_date")

    holidays: dict[date, set[str]] = {}
    for record in HOLIDAY_RULES:
        if record["algorithm"] in {
                "weekend", "observe_sunday_next_monday",
                "observe_saturday_previous_friday_unless_period_end"}:
            continue
        for year in range(first.year - 1, last.year + 2):
            value = _rule_occurrence(record, year)
            if value is not None and first <= value <= last:
                holidays.setdefault(value, set()).add(record["rule_id"])
    for record in special:
        if record["kind"] == "full_closure":
            value = date.fromisoformat(record["date"])
            if first <= value <= last:
                holidays.setdefault(value, set()).add("special:" + record["date"])

    early: dict[date, list[tuple[str, str]]] = {}
    for record in EARLY_CLOSE_RULES:
        for year in range(first.year, last.year + 1):
            value = _early_rule_occurrence(record, year)
            if (value is not None and first <= value <= last
                    and value.weekday() < 5 and value not in holidays):
                early.setdefault(value, []).append(
                    (record["close_et"], record["rule_id"]))
    for record in DATED_EARLY_CLOSE_EXCEPTIONS:
        value = date.fromisoformat(record["date"])
        if first <= value <= last:
            early.setdefault(value, []).append(
                (record["close_et"], "dated:" + record["date"]))
    for record in special:
        if record["kind"] == "early_close_unscheduled":
            value = date.fromisoformat(record["date"])
            if first <= value <= last:
                early.setdefault(value, []).append(
                    (record["close_et"], "special:" + record["date"]))

    rows = []
    for value in _date_range(first, last):
        if value.weekday() >= 5:
            row = {
                "date": value.isoformat(), "family": SESSION_FAMILY,
                "open_et": None, "close_et": None, "status": "closed_weekend",
            }
        elif value in holidays:
            row = {
                "date": value.isoformat(), "family": SESSION_FAMILY,
                "open_et": None, "close_et": None, "status": "closed_holiday",
            }
        else:
            directives = early.get(value, [])
            closes = {close for close, _source in directives}
            if len(closes) > 1:
                raise ValueError(
                    f"conflicting close directives on {value}: {directives}")
            close = next(iter(closes), REGULAR_SESSION["regular_close_et"])
            row = {
                "date": value.isoformat(), "family": SESSION_FAMILY,
                "open_et": _regular_open(value), "close_et": close,
                "status": ("open_regular" if close == "16:00"
                           else "open_early_close"),
            }
        if value in early and row["status"].startswith("closed_"):
            raise ValueError(
                f"early-close directive falls on closed date {value}: {early[value]}")
        rows.append(row)
    return rows


def load_and_check_evidence(path: Path = EVIDENCE_PATH) -> dict:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read evidence snapshot {path}: {exc}") from exc
    if not isinstance(payload, dict) or set(payload) != {
            "bank_root_label", "captured_as_of", "close_time_evidence",
            "consensus", "read_only_contract", "schema_version",
            "special_closure_sidecar", "trusted_heads"}:
        raise ValueError("evidence snapshot root schema drifted")
    if payload["schema_version"] != 1:
        raise ValueError("evidence snapshot schema version is not 1")
    _parse_date(payload["captured_as_of"], "evidence.captured_as_of")

    heads = payload["trusted_heads"]
    if (not isinstance(heads, dict)
            or heads.get("entry_count") != 505
            or heads.get("shape_counts") != {
                "identity_object": 7, "scalar": 498}
            or heads.get("oldest_date") != COVERAGE_START
            or heads.get("oldest_tickers") != ["BAC"]):
        raise ValueError("trusted-head evidence denominator or floor drifted")

    sidecar = payload["special_closure_sidecar"]
    if not isinstance(sidecar, dict) or sidecar.get("state") not in {
            "absent", "empty", "present"}:
        raise ValueError("special-closure sidecar snapshot is invalid")
    sidecar_dates = sidecar.get("dates")
    if (not isinstance(sidecar_dates, list)
            or sidecar_dates != sorted(set(sidecar_dates))):
        raise ValueError("special-closure sidecar dates are not sorted unique")
    for index, value in enumerate(sidecar_dates):
        _parse_date(value, f"evidence.special_closures[{index}]")
    canonical_sidecar = {"state": sidecar["state"], "dates": sidecar_dates}
    if sidecar.get("canonical_sha256") != _digest_value(canonical_sidecar):
        raise ValueError("special-closure sidecar canonical digest drifted")

    consensus = payload["consensus"]
    dates = consensus.get("positive_dates") if isinstance(consensus, dict) else None
    if (not isinstance(dates, list) or not dates
            or dates != sorted(set(dates))
            or consensus.get("positive_date_count") != len(dates)
            or consensus.get("min_tickers") != 2
            or consensus.get("positive_dates_sha256") != _digest_value(dates)):
        raise ValueError("consensus-positive evidence is malformed")
    for index, value in enumerate(dates):
        _parse_date(value, f"evidence.consensus[{index}]")
    if (consensus.get("first_positive_date") != dates[0]
            or consensus.get("last_positive_date") != dates[-1]):
        raise ValueError("consensus-positive evidence bounds drifted")

    close = payload["close_time_evidence"]
    close_dates = close.get("dates") if isinstance(close, dict) else None
    if (not isinstance(close_dates, list) or not close_dates
            or close.get("date_count") != len(close_dates)
            or close.get("dates_sha256") != _digest_value(close_dates)):
        raise ValueError("close-time evidence is malformed")
    seen = []
    for index, record in enumerate(close_dates):
        if not isinstance(record, dict) or set(record) != {
                "date", "final_nonzero_tickers_by_et",
                "ticker_count_with_nonzero"}:
            raise ValueError(f"close-time evidence {index}: schema drifted")
        _parse_date(record["date"], f"close-time evidence {index}.date")
        _decode_final_nonzero_groups(record, f"close-time evidence {index}")
        seen.append(record["date"])
    if seen != sorted(set(seen)):
        raise ValueError("close-time evidence dates are not sorted unique")
    if close.get("first_date") != seen[0] or close.get("last_date") != seen[-1]:
        raise ValueError("close-time evidence bounds drifted")
    return payload


def _load_market_calendar():
    spec = importlib.util.spec_from_file_location(
        "_row79_market_calendar", MARKET_CALENDAR_PATH)
    if spec is None or spec.loader is None:
        raise ValueError("cannot load shipped market_calendar cross-check")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _row_map(rows: list[dict]) -> dict[str, dict]:
    return {record["date"]: record for record in rows}


def _is_open(row: dict) -> bool:
    return row["status"] in {"open_regular", "open_early_close"}


def daily_settled_close_minutes(row: dict, *, margin_minutes: int | None = None) -> int:
    if not _is_open(row):
        raise ValueError("closed dates have no daily settled close")
    margin = (REGULAR_SESSION["daily_settle_margin_minutes"]
              if margin_minutes is None else margin_minutes)
    if isinstance(margin, bool) or not isinstance(margin, int) or margin < 0:
        raise ValueError("settle margin must be a nonnegative integer")
    return max(
        _minutes(row["close_et"], "row.close_et"),
        _minutes(
            REGULAR_SESSION["daily_settle_floor_close_et"],
            "daily_settle_floor_close_et"),
    ) + margin


def _check_crosschecks(
        rows: list[dict], evidence: dict, special: list[dict]
) -> dict:
    by_date = _row_map(rows)
    consensus = evidence["consensus"]
    for value in consensus["positive_dates"]:
        row = by_date.get(value)
        if row is None or not _is_open(row):
            raise ValueError(
                f"consensus-positive date is not open in schedule: {value}")

    late = {
        record["date"]: record["close_et"]
        for record in special if record["kind"] == "late_close_unscheduled"
    }
    close_checked = 0
    close_time_conflicts = []
    close_time_anomalies = []
    for record in evidence["close_time_evidence"]["dates"]:
        row = by_date.get(record["date"])
        if row is None:
            continue
        if not _is_open(row):
            raise ValueError(
                f"nonzero intraday evidence falls on closed date "
                f"{record['date']}")
        claimed = late.get(record["date"], row["close_et"])
        evaluation = evaluate_close_time_record(record, claimed)
        if evaluation["convicts"]:
            close_time_conflicts.append({
                "claimed_close_et": claimed,
                "date": record["date"],
                "denominator_ticker_count": evaluation[
                    "denominator_ticker_count"],
                "late_ticker_count": evaluation["late_ticker_count"],
                "threshold": evaluation["threshold"],
            })
        elif evaluation["late_ticker_count"]:
            close_time_anomalies.append(evaluation)
        close_checked += 1
    if close_time_conflicts:
        raise ValueError(
            "breadth close-time contradictions: "
            + json.dumps(
                close_time_conflicts,
                sort_keys=True,
                separators=(",", ":"),
            )
        )

    market_calendar = _load_market_calendar()
    sidecar_dates = {
        date.fromisoformat(value)
        for value in evidence["special_closure_sidecar"]["dates"]
    }
    first = date.fromisoformat(consensus["first_positive_date"])
    last = date.fromisoformat(consensus["last_positive_date"])
    calendar_checked = 0
    for value in _date_range(first, last):
        row = by_date.get(value.isoformat())
        if row is None:
            raise ValueError("market-calendar common span exceeds generated schedule")
        shipped_open = market_calendar.is_trading_day(
            value, special_closures=sidecar_dates)
        if shipped_open != _is_open(row):
            raise ValueError(
                f"market_calendar trading-day cross-check differs on {value}: "
                f"shipped={shipped_open}, generated={_is_open(row)}")
        calendar_checked += 1

    shipped_early = set(market_calendar.EARLY_CLOSE_DATES)
    early_first, early_last = min(shipped_early), max(shipped_early)
    generated_early = {
        date.fromisoformat(record["date"])
        for record in rows
        if (record["status"] == "open_early_close"
            and record["close_et"] == "13:00"
            and early_first <= date.fromisoformat(record["date"]) <= early_last)
    }
    if generated_early != shipped_early or len(generated_early) != 19:
        raise ValueError(
            f"EARLY_CLOSE_DATES identity differs: missing="
            f"{sorted(shipped_early - generated_early)}, extra="
            f"{sorted(generated_early - shipped_early)}")
    return {
        "consensus_positive_dates_checked": len(consensus["positive_dates"]),
        "close_time_dates_checked": close_checked,
        "below_threshold_close_anomalies": close_time_anomalies,
        "below_threshold_close_anomaly_dates": len(close_time_anomalies),
        "below_threshold_close_anomaly_tickers": sum(
            record["late_ticker_count"] for record in close_time_anomalies),
        "market_calendar_dates_checked": calendar_checked,
        "early_close_identity_count": len(generated_early),
    }


def check_close_time_breadth_obligations(
        rows: list[dict], evidence: dict
) -> dict:
    synthetic = (
        ("floor_one_ticker", 1, 1, 3, False),
        ("floor_threshold_minus_one", 3, 2, 3, False),
        ("floor_threshold_inclusive", 3, 3, 3, True),
        ("breadth_threshold_minus_one", 61, 3, 4, False),
        ("breadth_threshold_inclusive", 61, 4, 4, True),
        ("broad_exchange_archetype", 100, 99, 5, True),
    )
    for name, denominator, late_count, expected_threshold, expected_conviction \
            in synthetic:
        threshold = close_time_breadth_threshold(denominator)
        conviction = close_time_convicts(late_count, denominator)
        if threshold != expected_threshold or conviction is not expected_conviction:
            raise ValueError(
                f"G1 close-breadth {name}: threshold/conviction "
                f"{threshold}/{conviction} != "
                f"{expected_threshold}/{expected_conviction}")

    at_close_report = evaluate_close_time_record({
        "date": "2000-01-03",
        "final_nonzero_tickers_by_et": {"13:00": "AAA"},
        "ticker_count_with_nonzero": 1,
    }, "13:00")
    if (
        at_close_report["late_ticker_count"] != 1
        or at_close_report["late_tickers"] != [{
            "final_nonzero_et": "13:00", "ticker": "AAA"}]
    ):
        raise ValueError(
            "G1 close-breadth at_close_inclusive: final bar exactly at "
            "claimed close was not counted late")

    by_row = _row_map(rows)
    by_evidence = {
        record["date"]: record
        for record in evidence["close_time_evidence"]["dates"]
    }
    real_reports = []
    for value, expected in REAL_CLOSE_BREADTH_PINS.items():
        row = by_row.get(value)
        record = by_evidence.get(value)
        if row is None or record is None:
            raise ValueError(f"G1 close-breadth real pin {value}: missing input")
        if row["close_et"] != expected["claimed_close_et"]:
            raise ValueError(
                f"G1 close-breadth real pin {value}: close {row['close_et']} drifted")
        report = evaluate_close_time_record(record, row["close_et"])
        late_tickers = {
            item["ticker"]: item["final_nonzero_et"]
            for item in report["late_tickers"]
        }
        observed = {
            "denominator": report["denominator_ticker_count"],
            "late_tickers": late_tickers,
            "threshold": report["threshold"],
        }
        expected_observed = {
            "denominator": expected["denominator"],
            "late_tickers": expected["late_tickers"],
            "threshold": expected["threshold"],
        }
        if observed != expected_observed or report["convicts"]:
            raise ValueError(
                f"G1 close-breadth real pin {value}: {observed} != "
                f"{expected_observed}, convicts={report['convicts']}")
        real_reports.append(report)

    return {
        "mutation_kill_names": [
            "floor_3_to_4",
            "divisor_20_to_21",
            "inclusive_to_strict",
            "ceiling_to_floor",
            "at_after_to_strict_after",
        ],
        "obligation_count": len(synthetic) + 1 + len(real_reports),
        "real_false_conviction_pins": real_reports,
        "synthetic_case_count": len(synthetic) + 1,
    }


def _require_row(
        by_date: dict[str, dict], value: str, *, status: str,
        open_et: str | None = None, close_et: str | None = None,
) -> None:
    row = by_date.get(value)
    if row is None or row["status"] != status:
        raise ValueError(
            f"G1 obligation {value}: expected {status}, got {row}")
    if open_et is not None and row["open_et"] != open_et:
        raise ValueError(
            f"G1 obligation {value}: expected open {open_et}, got {row['open_et']}")
    if close_et is not None and row["close_et"] != close_et:
        raise ValueError(
            f"G1 obligation {value}: expected close {close_et}, got {row['close_et']}")


def check_g1_obligations(
        rows: list[dict], special: list[dict], evidence: dict
) -> dict:
    by_date = _row_map(rows)
    obligations = 0
    emitted: set[str] = set()

    weekend = next(
        value for value in _date_range(
            date.fromisoformat(COVERAGE_START),
            date.fromisoformat(COVERAGE_START) + timedelta(days=7))
        if value.weekday() >= 5)
    _require_row(by_date, weekend.isoformat(), status="closed_weekend")
    emitted.add("weekend")
    obligations += 1
    _require_row(by_date, "2023-01-02", status="closed_holiday")
    emitted.add("shift_sunday")
    obligations += 1
    _require_row(by_date, "2021-12-24", status="closed_holiday")
    _require_row(
        by_date, "2021-12-31", status="open_regular", close_et="16:00")
    emitted.add("shift_saturday")
    obligations += 2

    for record in HOLIDAY_RULES:
        if record["rule_id"] in emitted:
            continue
        values = [
            _rule_occurrence(record, year)
            for year in range(
                date.fromisoformat(COVERAGE_START).year,
                date.fromisoformat(rows[-1]["date"]).year + 1)
        ]
        value = next((item for item in values if item is not None), None)
        if value is None:
            raise ValueError(
                f"G1 denominator emitted no obligation for {record['rule_id']}")
        _require_row(by_date, value.isoformat(), status="closed_holiday")
        emitted.add(record["rule_id"])
        obligations += 1

    for record in EXPLICIT_NON_HOLIDAY_RULES:
        value = next(
            (_rule_occurrence(record, year, shifted=False)
             for year in range(1980, 1990)
             if _rule_occurrence(record, year, shifted=False) is not None
             and _is_open(by_date[_rule_occurrence(
                 record, year, shifted=False).isoformat()])),
            None,
        )
        if value is None:
            raise ValueError(
                f"G1 denominator emitted no open obligation for {record['rule_id']}")
        emitted.add(record["rule_id"])
        obligations += 1

    for record in EARLY_CLOSE_RULES:
        value = next(
            (_early_rule_occurrence(record, year)
             for year in range(1980, date.fromisoformat(rows[-1]["date"]).year + 1)
             if _early_rule_occurrence(record, year) is not None),
            None,
        )
        if value is None:
            raise ValueError(
                f"G1 denominator emitted no early obligation for {record['rule_id']}")
        _require_row(
            by_date, value.isoformat(), status="open_early_close",
            close_et=record["close_et"])
        emitted.add(record["rule_id"])
        obligations += 1

    expected_rules = {
        record["rule_id"]
        for record in HOLIDAY_RULES + EXPLICIT_NON_HOLIDAY_RULES + EARLY_CLOSE_RULES
    }
    if emitted != expected_rules:
        raise ValueError(
            f"G1 rule denominator drift: missing={sorted(expected_rules - emitted)}, "
            f"extra={sorted(emitted - expected_rules)}")

    for record in special:
        if record["kind"] == "full_closure":
            _require_row(by_date, record["date"], status="closed_holiday")
        elif record["kind"] == "early_close_unscheduled":
            _require_row(
                by_date, record["date"], status="open_early_close",
                close_et=record["close_et"])
        else:
            _require_row(
                by_date, record["date"], status="open_regular", close_et="16:00")
        obligations += 1

    evidence_dates = {
        record["date"] for record in evidence["close_time_evidence"]["dates"]
    }
    evidence_first = date.fromisoformat(
        evidence["close_time_evidence"]["first_date"])
    evidence_last = date.fromisoformat(
        evidence["close_time_evidence"]["last_date"])
    full_closure_zero_bar_pins = []
    for record in special:
        value = date.fromisoformat(record["date"])
        if (record["kind"] == "full_closure"
                and evidence_first <= value <= evidence_last):
            if record["date"] in evidence_dates:
                raise ValueError(
                    f"G1 full closure {record['date']} has nonzero 1m bank evidence")
            full_closure_zero_bar_pins.append(record["date"])
    obligations += len(full_closure_zero_bar_pins)

    for record in DATED_EARLY_CLOSE_EXCEPTIONS:
        _require_row(
            by_date, record["date"], status="open_early_close",
            close_et=record["close_et"])
        obligations += 1
    for record in CHRISTMAS_EVE_GAP_OBLIGATIONS:
        _require_row(by_date, record["date"], status=record["expected"])
        obligations += 1

    _require_row(by_date, "1997-01-20", status="open_regular")
    _require_row(by_date, "1998-01-19", status="closed_holiday")
    _require_row(by_date, "2021-06-18", status="open_regular")
    _require_row(by_date, "2021-06-21", status="open_regular")
    _require_row(by_date, "2022-06-20", status="closed_holiday")
    _require_row(by_date, "1980-11-04", status="closed_holiday")
    _require_row(by_date, "1984-11-06", status="open_regular")
    _require_row(
        by_date, "1992-11-27", status="open_early_close", close_et="14:00")
    _require_row(
        by_date, "1993-11-26", status="open_early_close", close_et="13:00")
    _require_row(
        by_date, "1995-07-03", status="open_early_close", close_et="13:00")
    _require_row(
        by_date, "1996-12-24", status="open_early_close", close_et="13:00")
    _require_row(
        by_date, "2012-11-23", status="open_early_close", close_et="13:00")
    _require_row(
        by_date, "2025-12-24", status="open_early_close", close_et="13:00")
    _require_row(
        by_date, "1985-09-26", status="open_regular", open_et="10:00")
    _require_row(by_date, "1985-09-27", status="closed_holiday")
    _require_row(
        by_date, "1985-09-30", status="open_regular", open_et="09:30")
    obligations += 17

    late = by_date["2009-07-02"]
    if daily_settled_close_minutes(late) != 16 * 60 + 20:
        raise ValueError("2009 late-close daily floor does not settle at 16:20")
    if daily_settled_close_minutes(late, margin_minutes=14) > 16 * 60 + 15:
        raise ValueError("2009 late-close margin mutation did not turn red")
    obligations += 2
    breadth = check_close_time_breadth_obligations(rows, evidence)
    obligations += breadth["obligation_count"]
    return {
        "close_time_breadth": breadth,
        "full_closure_zero_bar_count": len(full_closure_zero_bar_pins),
        "full_closure_zero_bar_dates": full_closure_zero_bar_pins,
        "obligation_count": obligations,
        "rule_input_count": len(expected_rules),
        "special_entry_count": len(special),
        "dated_exception_count": len(DATED_EARLY_CLOSE_EXCEPTIONS),
        "christmas_gap_count": len(CHRISTMAS_EVE_GAP_OBLIGATIONS),
    }


def _validate_rows(rows: list[dict], first_date: str, valid_through: str) -> dict:
    if not isinstance(rows, list) or not rows:
        raise ValueError("schedule rows must be a nonempty array")
    first = date.fromisoformat(first_date)
    last = date.fromisoformat(valid_through)
    expected_dates = [value.isoformat() for value in _date_range(first, last)]
    raw_keys = []
    actual_dates = []
    statuses = Counter()
    for index, row in enumerate(rows):
        if not isinstance(row, dict) or set(row) != {
                "close_et", "date", "family", "open_et", "status"}:
            raise ValueError(f"schedule row {index}: schema drifted")
        _parse_date(row["date"], f"schedule row {index}.date")
        if row["family"] != SESSION_FAMILY:
            raise ValueError(f"schedule row {index}: unknown family")
        status = row["status"]
        if status not in {
                "open_regular", "open_early_close",
                "closed_holiday", "closed_weekend"}:
            raise ValueError(f"schedule row {index}: invalid status")
        if status.startswith("open_"):
            _check_time(row["open_et"], f"schedule row {index}.open_et")
            _check_time(row["close_et"], f"schedule row {index}.close_et")
            if _minutes(row["open_et"], "open") >= _minutes(row["close_et"], "close"):
                raise ValueError(f"schedule row {index}: open is not before close")
        elif row["open_et"] is not None or row["close_et"] is not None:
            raise ValueError(f"schedule row {index}: closed row carries times")
        raw_keys.append((row["family"], row["date"]))
        actual_dates.append(row["date"])
        statuses[status] += 1
    if len(raw_keys) != len(set(raw_keys)):
        raise ValueError("schedule contains duplicate raw (family,date) keys")
    if actual_dates != expected_dates:
        missing = sorted(set(expected_dates) - set(actual_dates))[:10]
        extra = sorted(set(actual_dates) - set(expected_dates))[:10]
        raise ValueError(
            f"schedule date set/order differs: missing={missing}, extra={extra}")
    return dict(sorted(statuses.items()))


def build_table(
        *, source_commit: str, generated_as_of: str, valid_through: str,
        evidence_path: Path = EVIDENCE_PATH,
) -> tuple[dict, dict]:
    if not isinstance(source_commit, str) or not SOURCE_COMMIT_RE.fullmatch(source_commit):
        raise ValueError("generator source commit must be a full lowercase Git hash")
    as_of = _parse_date(generated_as_of, "generated_as_of")
    through = _parse_date(valid_through, "valid_through")
    if through < as_of + timedelta(days=MIN_GENERATION_RUNWAY_DAYS):
        raise ValueError(
            f"valid_through must provide at least {MIN_GENERATION_RUNWAY_DAYS} days runway")
    input_report = check_inputs()
    if PLANNER_INPUT_FINDINGS:
        raise ValueError("unresolved planner input finding blocks table generation")
    special = load_and_check_special_closures()
    evidence = load_and_check_evidence(evidence_path)
    head_floor = as_of.replace(year=as_of.year - 30)
    required_floor = min(
        date.fromisoformat(evidence["trusted_heads"]["oldest_date"]), head_floor)
    if required_floor < date.fromisoformat(COVERAGE_START):
        raise ValueError(
            f"trusted/probe floor {required_floor} predates rule coverage {COVERAGE_START}")
    if required_floor.isoformat() != COVERAGE_START:
        raise ValueError(
            f"coverage start {COVERAGE_START} is not the derived required floor "
            f"{required_floor}")

    rows = generate_rows(COVERAGE_START, valid_through, special)
    status_counts = _validate_rows(rows, COVERAGE_START, valid_through)
    crosschecks = _check_crosschecks(rows, evidence, special)
    obligations = check_g1_obligations(rows, special, evidence)
    rule_payload = {
        "holiday_rules": HOLIDAY_RULES,
        "explicit_non_holiday_rules": EXPLICIT_NON_HOLIDAY_RULES,
        "early_close_rules": EARLY_CLOSE_RULES,
        "dated_early_close_exceptions": DATED_EARLY_CLOSE_EXCEPTIONS,
        "christmas_eve_gap_obligations": CHRISTMAS_EVE_GAP_OBLIGATIONS,
        "regular_session": REGULAR_SESSION,
        "regular_open_transitions": REGULAR_OPEN_TRANSITIONS,
        "mandatory_pins": MANDATORY_PINS,
    }
    evidence_digest = _digest_value(evidence)
    table = {
        "schema_version": TABLE_SCHEMA_VERSION,
        "header": {
            "authority": "NYSE committed in-repo stdlib schedule",
            "family": SESSION_FAMILY,
            "timezone": REGULAR_SESSION["timezone"],
            "coverage": {
                "first_date": COVERAGE_START,
                "valid_through": valid_through,
                "required_generation_runway_days": MIN_GENERATION_RUNWAY_DAYS,
            },
            "generated_as_of": generated_as_of,
            "generator_source_commit": source_commit,
            "provenance": {
                "appendix_j_input_sha256": input_report["input_digest_sha256"],
                "rule_set_sha256": _digest_value(rule_payload),
                "special_closure_list_sha256": hashlib.sha256(
                    SPECIAL_CLOSURES_PATH.read_bytes()).hexdigest(),
                "evidence_snapshot_sha256": evidence_digest,
                "consensus_dataset_sha256": evidence["consensus"][
                    "positive_dates_sha256"],
                "special_closure_sidecar_snapshot_sha256": evidence[
                    "special_closure_sidecar"]["canonical_sha256"],
            },
            "settlement": {
                "regular_close_et": REGULAR_SESSION["regular_close_et"],
                "daily_settle_floor_close_et": REGULAR_SESSION[
                    "daily_settle_floor_close_et"],
                "daily_settle_margin_minutes": REGULAR_SESSION[
                    "daily_settle_margin_minutes"],
            },
            "row_count": len(rows),
            "status_counts": status_counts,
            "update_rule": (
                "extend valid_through only by reviewed regeneration from committed "
                "rules, closures, and evidence; queries outside bounds are refused"),
        },
        "rows": rows,
    }
    report = {
        "status": "ok",
        "generated_schedule": True,
        "row_count": len(rows),
        "first_date": COVERAGE_START,
        "valid_through": valid_through,
        "status_counts": status_counts,
        "input_digest_sha256": input_report["input_digest_sha256"],
        "evidence_snapshot_sha256": evidence_digest,
        "crosschecks": crosschecks,
        "g1": obligations,
        "table_sha256": _digest_value(table),
    }
    return table, report


def check_table(path: Path = TABLE_PATH, *, evidence_path: Path = EVIDENCE_PATH) -> dict:
    try:
        actual = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read schedule table {path}: {exc}") from exc
    if not isinstance(actual, dict) or set(actual) != {"header", "rows", "schema_version"}:
        raise ValueError("schedule table root schema drifted")
    header = actual.get("header")
    if not isinstance(header, dict):
        raise ValueError("schedule table header is missing")
    coverage = header.get("coverage")
    if not isinstance(coverage, dict):
        raise ValueError("schedule table coverage header is missing")
    expected, report = build_table(
        source_commit=header.get("generator_source_commit"),
        generated_as_of=header.get("generated_as_of"),
        valid_through=coverage.get("valid_through"),
        evidence_path=evidence_path,
    )
    if actual != expected:
        raise ValueError("committed schedule table differs from deterministic regeneration")
    report["checked_table"] = str(path)
    return report


def _write_json(path: Path, payload: dict) -> None:
    path = path.resolve()
    try:
        path.relative_to((PROJECT_ROOT / "Stock Data Storage").resolve())
    except ValueError:
        pass
    else:
        raise ValueError("schedule output must not be inside Stock Data Storage")
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
    actions = parser.add_mutually_exclusive_group(required=True)
    actions.add_argument("--check-inputs", action="store_true")
    actions.add_argument("--check-evidence", action="store_true")
    actions.add_argument("--generate", action="store_true")
    actions.add_argument("--check-table", action="store_true")
    parser.add_argument("--evidence", type=Path, default=EVIDENCE_PATH)
    parser.add_argument("--output", type=Path, default=TABLE_PATH)
    parser.add_argument("--source-commit")
    parser.add_argument("--as-of")
    parser.add_argument("--valid-through")
    args = parser.parse_args(argv)
    try:
        if args.check_inputs:
            result = check_inputs()
        elif args.check_evidence:
            evidence = load_and_check_evidence(args.evidence)
            result = {
                "status": "ok",
                "evidence_snapshot_sha256": _digest_value(evidence),
                "trusted_head_count": evidence["trusted_heads"]["entry_count"],
                "consensus_positive_dates": evidence["consensus"][
                    "positive_date_count"],
                "close_time_dates": evidence["close_time_evidence"]["date_count"],
            }
        elif args.generate:
            if not args.source_commit or not args.as_of or not args.valid_through:
                parser.error(
                    "--generate requires --source-commit, --as-of, and --valid-through")
            table, result = build_table(
                source_commit=args.source_commit,
                generated_as_of=args.as_of,
                valid_through=args.valid_through,
                evidence_path=args.evidence,
            )
            _write_json(args.output, table)
            result["output"] = str(args.output)
        else:
            result = check_table(args.output, evidence_path=args.evidence)
    except ValueError as exc:
        parser.exit(2, f"schedule check failed: {exc}\n")
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
