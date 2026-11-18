"""NYSE early-close (half-day) calendar — stdlib-only, self-contained.

Purpose: let a future span/completeness check NOT false-alarm on
legitimate half-days. On a 1:00pm-ET early close the regular session
ends three hours sooner, so the day's last bar lands at 13:00 instead
of 16:00. Without this knowledge a "missing afternoon bars" check would
flag every Black Friday / Christmas Eve / July-3 session as a gap. This
module answers the single question "was this date a 1:00pm early close?"
and what close time to EXPECT.

No dependency on any other engine module; only datetime from the stdlib.

The recurring NYSE 1:00pm early closes are:
  - the day AFTER Thanksgiving (Black Friday),
  - Christmas Eve, Dec 24, when it is a weekday,
  - July 3, the session before Independence Day, when it is a weekday.
When Dec 24 / July 3 fall on a weekend there may be no early close; an
exchange holiday is not evidence that the prior session closes early. The
dates are pinned by hand below rather than inferred.
"""

import datetime
import json
from pathlib import Path


# ===========================================================================
# ANNUAL REVIEW — add the new year's THREE half-days here (Black Friday, the
# weekday Dec 24, and the weekday July 3). Each must be a weekday; when Dec 24
# or July 3 lands on a weekend, do not infer an observed early close: look up
# the official NYSE holiday calendar rather than guessing. Keep this list
# sorted by date.
# ===========================================================================
EARLY_CLOSE_DATES = frozenset({
    # --- 2018 ---
    datetime.date(2018, 11, 23),   # Black Friday
    datetime.date(2018, 12, 24),   # Christmas Eve (Mon)
    # --- 2019 ---
    datetime.date(2019, 7, 3),     # July 3 (Wed)
    datetime.date(2019, 11, 29),   # Black Friday
    datetime.date(2019, 12, 24),   # Christmas Eve (Tue)
    # --- 2020 ---
    datetime.date(2020, 11, 27),   # Black Friday
    datetime.date(2020, 12, 24),   # Christmas Eve (Thu)
    # (July 3 2020 fell on Fri but was the OBSERVED July-4 holiday: closed)
    # --- 2021 ---
    datetime.date(2021, 11, 26),   # Black Friday
    # (Dec 24 2021 fell on Fri = observed Christmas holiday: full close)
    # (July 3 2021 fell on Sat: no session)
    # --- 2022 ---
    datetime.date(2022, 11, 25),   # Black Friday
    # (Dec 24 2022 fell on Sat: no session; July 3 2022 fell on Sun)
    # --- 2023 ---
    datetime.date(2023, 7, 3),     # July 3 (Mon)
    datetime.date(2023, 11, 24),   # Black Friday
    # (Dec 24 was Sun; Fri Dec 22 was a normal full session, not an early close)
    # --- 2024 ---
    datetime.date(2024, 7, 3),     # July 3 (Wed)
    datetime.date(2024, 11, 29),   # Black Friday
    datetime.date(2024, 12, 24),   # Christmas Eve (Tue)
    # --- 2025 ---
    datetime.date(2025, 7, 3),     # July 3 (Thu)
    datetime.date(2025, 11, 28),   # Black Friday
    datetime.date(2025, 12, 24),   # Christmas Eve (Wed)
    # --- 2026 ---
    datetime.date(2026, 11, 27),   # Black Friday
    datetime.date(2026, 12, 24),   # Christmas Eve (Thu)
    # (July 3 2026 falls on Fri = observed July-4 holiday: full close, no half-day)
})


def _as_date(d):
    """Coerce a date or datetime to a plain date; reject anything else."""
    if isinstance(d, datetime.datetime):
        return d.date()
    if isinstance(d, datetime.date):
        return d
    raise TypeError(f"expected date or datetime, got {type(d).__name__}")


def is_early_close(d):
    """True iff `d` (a date or datetime, compared by date) is a known NYSE
    1:00pm early-close session."""
    return _as_date(d) in EARLY_CLOSE_DATES


def early_close_time():
    """The local-ET wall-clock close on a half-day: 1:00pm."""
    return datetime.time(13, 0)


def regular_close_time():
    """The local-ET wall-clock close on a normal session: 4:00pm."""
    return datetime.time(16, 0)


def expected_close(d):
    """The close time to EXPECT for `d`: 13:00 on a half-day, else 16:00."""
    return early_close_time() if is_early_close(d) else regular_close_time()


# ===========================================================================
# Full-closure (no-session) NYSE holidays. Most full holidays follow precise
# rules, so they are COMPUTED (valid for any year, no annual review): the
# floating Monday/Thursday holidays, Good Friday (Easter - 2), and the fixed-date
# holidays with NYSE weekend observance (Sat -> the preceding Friday, Sun -> the
# following Monday). New Year's Day is the one exception NYSE does NOT move to
# the preceding Friday when it lands on a Saturday. Juneteenth became an NYSE
# holiday in 2022. One-off closures are pinned below.
#
# These let is_trading_day() tell a real trading day from a holiday so a
# completeness check can flag a genuinely DROPPED session, not a holiday.
# ===========================================================================
SPECIAL_FULL_CLOSE_DATES = frozenset({
    datetime.date(2012, 10, 29),  # Hurricane Sandy — NYSE closed (day 1 of 2)
    datetime.date(2012, 10, 30),  # Hurricane Sandy — NYSE closed (day 2 of 2)
    datetime.date(2018, 12, 5),   # National Day of Mourning — Pres. G.H.W. Bush
    datetime.date(2025, 1, 9),    # National Day of Mourning — Pres. Carter
})
# NOTE: these span the bank's ~2011+ range. Earlier ad-hoc NYSE closures
# (9/11 2001-09-11..14, Reagan 2004-06-11, Ford 2007-01-02) are intentionally
# omitted — no stored history reaches them; add them if the bank ever goes deeper.

SPECIAL_CLOSURES_SIDECAR = "_special_closures.json"
_SIDECAR_FULL_CLOSE_DATES = frozenset()
_SIDECAR_PATH = None
_SIDECAR_AUTOLOAD_TRIED = False


def _parse_iso_date(value):
    try:
        if isinstance(value, datetime.datetime):
            return value.date()
        if isinstance(value, datetime.date):
            return value
        return datetime.date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return None


def _closure_dates_from_payload(payload, *, strict=False):
    """Read every supported sidecar shape into a date set.

    Production writes {"closures": [{"date": "YYYY-MM-DD", ...}, ...]}.
    The looser parser keeps the calendar tolerant if a human trims the file to
    {"closures": ["YYYY-MM-DD"]} or {"dates": [...]}.
    """
    if not isinstance(payload, dict):
        if strict:
            raise ValueError("special-closure sidecar must be an object")
        return frozenset()
    if strict and not ({"closures", "dates"} & set(payload)):
        raise ValueError("special-closure sidecar has no closure list")
    raw = payload.get("closures", payload.get("dates", []))
    dates = set()
    if isinstance(raw, dict):
        raw = list(raw.values())
    if not isinstance(raw, list):
        if strict:
            raise ValueError("special-closure list is invalid")
        return frozenset()
    for item in raw:
        if isinstance(item, dict):
            if strict and "date" not in item:
                raise ValueError("special-closure entry has no date")
            item = item.get("date")
        d = _parse_iso_date(item)
        if d is None:
            if strict:
                raise ValueError("special-closure entry has an invalid date")
            continue
        if strict and (not isinstance(item, str) or item != d.isoformat()):
            raise ValueError("special-closure date is not canonical ISO format")
        dates.add(d)
    return frozenset(dates)


def _sidecar_path(root=None, path=None):
    if path is not None:
        return Path(path)
    if root is not None:
        return Path(root) / SPECIAL_CLOSURES_SIDECAR
    return (Path(__file__).resolve().parent.parent / "Stock Data Storage"
            / SPECIAL_CLOSURES_SIDECAR)


def read_special_closures(root=None, path=None, *, strict=False):
    """Return one bank's sidecar closures without changing process globals.

    The default remains backwards-compatible and treats an unavailable or
    malformed optional sidecar as empty.  Safety-sensitive callers may request
    ``strict=True``: a missing file is still an empty optional dependency, but
    any existing unreadable or malformed file raises instead of silently
    changing session adjacency.
    """
    p = _sidecar_path(root=root, path=path)
    try:
        payload = json.loads(p.read_text(encoding="utf-8"))
        return _closure_dates_from_payload(payload, strict=strict)
    except FileNotFoundError:
        return frozenset()
    except (OSError, ValueError):
        if strict:
            raise
        return frozenset()


def load_special_closures(root=None, path=None):
    """Load bank-root `_special_closures.json` into this process.

    Missing/corrupt files are treated as "no extra closures"; the hardcoded
    SPECIAL_FULL_CLOSE_DATES remain active. Returns the loaded date set.
    """
    global _SIDECAR_FULL_CLOSE_DATES, _SIDECAR_PATH, _SIDECAR_AUTOLOAD_TRIED
    p = _sidecar_path(root=root, path=path)
    dates = read_special_closures(path=p)
    _SIDECAR_FULL_CLOSE_DATES = frozenset(dates)
    _SIDECAR_PATH = str(p)
    _SIDECAR_AUTOLOAD_TRIED = True
    _HOLIDAY_CACHE.clear()
    return _SIDECAR_FULL_CLOSE_DATES


def clear_special_closures():
    """Test/helper hook: clear only sidecar closures, never hardcoded ones."""
    global _SIDECAR_FULL_CLOSE_DATES, _SIDECAR_PATH, _SIDECAR_AUTOLOAD_TRIED
    _SIDECAR_FULL_CLOSE_DATES = frozenset()
    _SIDECAR_PATH = None
    _SIDECAR_AUTOLOAD_TRIED = False
    _HOLIDAY_CACHE.clear()


def loaded_special_closures():
    return _SIDECAR_FULL_CLOSE_DATES


def _autoload_special_closures():
    global _SIDECAR_AUTOLOAD_TRIED
    if _SIDECAR_AUTOLOAD_TRIED:
        return
    load_special_closures()


def _nth_weekday(year, month, weekday, n):
    """The n-th `weekday` (Mon=0 .. Sun=6) of (year, month), 1-based n."""
    first = datetime.date(year, month, 1)
    shift = (weekday - first.weekday()) % 7
    return first + datetime.timedelta(days=shift + 7 * (n - 1))


def _last_weekday(year, month, weekday):
    """The LAST `weekday` of (year, month)."""
    nxt = (datetime.date(year + 1, 1, 1) if month == 12
           else datetime.date(year, month + 1, 1))
    last = nxt - datetime.timedelta(days=1)
    return last - datetime.timedelta(days=(last.weekday() - weekday) % 7)


def _easter(year):
    """Gregorian Easter Sunday (Anonymous/Meeus algorithm)."""
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
    return datetime.date(year, month, day)


def _observed_fixed(d):
    """NYSE weekend observance for a FIXED-date holiday: Sat -> Fri, Sun ->
    Mon (used for Juneteenth, July 4, Christmas — NOT New Year's Day)."""
    if d.weekday() == 5:
        return d - datetime.timedelta(days=1)
    if d.weekday() == 6:
        return d + datetime.timedelta(days=1)
    return d


def _holidays_for_year_with(year, sidecar_dates):
    out = {d for d in SPECIAL_FULL_CLOSE_DATES if d.year == year}
    out.update(d for d in sidecar_dates if d.year == year)
    ny = datetime.date(year, 1, 1)             # New Year's Day (special)
    if ny.weekday() == 6:                      # Sunday -> Monday
        out.add(datetime.date(year, 1, 2))
    elif ny.weekday() != 5:                    # Sat is NOT observed; else as-is
        out.add(ny)
    out.add(_nth_weekday(year, 1, 0, 3))       # MLK Day (3rd Mon Jan)
    out.add(_nth_weekday(year, 2, 0, 3))       # Washington's Bday (3rd Mon Feb)
    out.add(_easter(year) - datetime.timedelta(days=2))   # Good Friday
    out.add(_last_weekday(year, 5, 0))         # Memorial Day (last Mon May)
    if year >= 2022:                           # Juneteenth (NYSE from 2022)
        out.add(_observed_fixed(datetime.date(year, 6, 19)))
    out.add(_observed_fixed(datetime.date(year, 7, 4)))   # Independence Day
    out.add(_nth_weekday(year, 9, 0, 1))       # Labor Day (1st Mon Sep)
    out.add(_nth_weekday(year, 11, 3, 4))      # Thanksgiving (4th Thu Nov)
    out.add(_observed_fixed(datetime.date(year, 12, 25)))  # Christmas
    return frozenset(out)


def _holidays_for_year(year):
    _autoload_special_closures()
    return _holidays_for_year_with(year, _SIDECAR_FULL_CLOSE_DATES)


_HOLIDAY_CACHE = {}


def _holidays(year):
    h = _HOLIDAY_CACHE.get(year)
    if h is None:
        h = _holidays_for_year(year)
        _HOLIDAY_CACHE[year] = h
    return h


def is_holiday(d):
    """True iff `d` is a full-closure NYSE holiday (no session that day)."""
    d = _as_date(d)
    return d in _holidays(d.year)


def is_trading_day(d, *, special_closures=None):
    """True iff `d` is a normal NYSE session day — a weekday that is not a
    full holiday. (A half-day IS a trading day; it just closes early.)"""
    d = _as_date(d)
    holidays = (_holidays(d.year) if special_closures is None
                else _holidays_for_year_with(d.year, special_closures))
    return d.weekday() < 5 and d not in holidays
